from __future__ import unicode_literals
import os
import sys
import inspect
import time
import gevent
import socket
import json
import logging
from six import text_type
from cStringIO import StringIO

from tornado.httpclient import HTTPError

from contrail_api_cli.command import Command, Option
from contrail_api_cli.exceptions import CommandError, NotFound
from contrail_api_cli.manager import CommandManager
from contrail_api_cli.utils import printo
from contrail_api_cli.resource import Resource

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.strategies import SubgraphStrategy
from gremlin_python.process.graph_traversal import __

from prometheus_client import start_http_server
from prometheus_client import Gauge

from . import utils
from .checks import *


avail_checks = [(name, obj) for name, obj in inspect.getmembers(sys.modules[__name__])
                if inspect.isfunction(obj) and name.startswith('check_')]
avail_tests = [(name, obj) for name, obj in inspect.getmembers(sys.modules[__name__])
               if inspect.isfunction(obj) and name.startswith('test_')]
avail_cleans = [(name, obj) for name, obj in inspect.getmembers(sys.modules[__name__])
                if inspect.isfunction(obj) and name.startswith('clean_')]

gauges = {}


class ResourceEncoder(json.JSONEncoder):
    def default(self, r):
        if isinstance(r, Resource):
            return {"type": r.type, "uuid": r.uuid, "fq_name": str(r.fq_name)}


class Fsck(Command):
    description = 'Checks and optionally clean API inconsistencies'
    gremlin_server = Option(default=os.environ.get('GREMLIN_FSCK_SERVER', 'localhost:8182'),
                            help='host:port of gremlin serveri (default: %(default)s)')
    checks = Option(help='Name of checks to run',
                    nargs='*', choices=[n[6:] for n, o in avail_checks],
                    default=[n[6:] for n, o in avail_checks],
                    metavar='check')
    tests = Option(help='Name of tests to run',
                   nargs='*', choices=[n[5:] for n, o in avail_tests] + ['all'],
                   default=[],
                   metavar='test')
    clean = Option(help='Run cleans (default: %(default)s)',
                   action='store_true',
                   default=bool(int(os.environ.get('GREMLIN_FSCK_CLEAN', 0))))
    loop = Option(help='Run in loop (default: %(default)s)',
                  action='store_true',
                  default=bool(int(os.environ.get('GREMLIN_FSCK_LOOP', 0))))
    loop_interval = Option(help='Interval between loops in seconds (default: %(default)s)',
                           default=os.environ.get('GREMLIN_FSCK_LOOP_INTERVAL', 60 * 5),
                           type=float)
    json = Option(help='Output logs in json',
                  action='store_true',
                  default=bool(int(os.environ.get('GREMLIN_FSCK_JSON', 0))))
    zk_server = Option(help="Zookeeper server (default: %(default)s)",
                       default=os.environ.get('GREMLIN_FSCK_ZK_SERVER', 'localhost:2181'))
    prometheus_port = Option(help="Prometheus endpoint port (default: %(default)s)",
                             default=os.environ.get('GREMLIN_FSCK_PROM_PORT', 8000),
                             type=int)
    prometheus_prefix = Option(help="Prometheus metrics prefix (default: %(default)s)",
                             default=os.environ.get('GREMLIN_FSCK_PROM_PREFIX', 'contrail'))

    def _check_by_name(self, name):
        c = None
        for n, check in avail_checks:
            if not name == n[6:]:
                continue
            else:
                c = check
        if c is None:
            raise CommandError("Can't find %s check method" % name)
        return c

    def _test_by_name(self, name):
        for n, test in avail_tests:
            if name == n[5:]:
                return test

    def _clean_by_name(self, name):
        c = None
        for n, clean in avail_cleans:
            if not name == n[6:]:
                continue
            else:
                c = clean
                break
        if c is None:
            raise CommandError("Can't find %s clean method" % name)
        return c

    def __call__(self, gremlin_server=None, checks=None, tests=None, clean=False,
                 loop=False, loop_interval=None, json=False, zk_server=False,
                 prometheus_port=8000, prometheus_prefix=None):
        if clean:
            CommandManager().load_namespace('contrail_api_cli.clean')
        utils.JSON_OUTPUT = json
        utils.ZK_SERVER = zk_server
        self.gremlin_server = gremlin_server
        self.prometheus_prefix = prometheus_prefix
        if tests:
            self.run_tests(tests)
        else:
            if loop is True:
                start_http_server(prometheus_port)
                self.run_loop(checks, clean, loop_interval)
            else:
                self.run(checks, clean)

    def get_traversal(self):
        graph = Graph()
        try:
            # take only non deleted resources
            return graph.traversal().withRemote(
                DriverRemoteConnection('ws://%s/gremlin' % self.gremlin_server, 'g')
            ).withStrategies(
                SubgraphStrategy(vertices=__.has('deleted', 0))
            )
        except (HTTPError, socket.error) as e:
            raise CommandError('Failed to connect to Gremlin server: %s' % e)

    def run_loop(self, checks, clean, loop_interval):
        while True:
            self.run(checks, clean)
            gevent.sleep(loop_interval)

    def run_tests(self, tests):
        utils.log('Running tests...')
        graph = Graph()
        g = graph.traversal().withRemote(
            DriverRemoteConnection('ws://%s/gremlin' % self.gremlin_server, 'g')
        )
        if 'all' in tests:
            tests = [n[5:] for n, _ in avail_tests]
        for test_name in tests:
            test_func = self._test_by_name(test_name)
            try:
                g.V().drop().iterate()
                test_func(g)
            except AssertionError as e:
                utils.log("Test %s failed: %s" % (test_name, e))
                sys.exit(1)

    def _run_check(self, check_name, g):
        check_func = self._check_by_name(check_name)
        old_stdout = sys.stdout
        sys.stdout = my_stdout = StringIO()
        success = True

        def cleanup():
            sys.stdout = old_stdout
            output = my_stdout.getvalue()
            my_stdout.close()
            return output

        start = time.time()
        try:
            r = check_func(g)
        except (Exception, NotFound) as e:
            output = text_type(e)
            total = -1
            success = False
            cleanup()
        else:
            if isinstance(r, list):
                total = len(r)
            else:
                total = 1
            if utils.JSON_OUTPUT:
                output = r
                cleanup()
            else:
                output = cleanup()
        end = time.time()

        gauge_name = "%s_%s" % (self.prometheus_prefix, check_name)
        if gauge_name not in gauges:
            gauges[gauge_name] = Gauge(gauge_name, check_func.__doc__.strip())
        gauges[gauge_name].set(total)

        if utils.JSON_OUTPUT:
            check_status = {
                "type": check_func.__name__.split('_')[0],
                "name": check_func.__name__,
                "total": total,
                "output": output,
                "success": success,
                "duration": "%0.2f ms" % ((end - start) * 1000.0)
            }
            printo(json.dumps(check_status, cls=ResourceEncoder))
        elif output:
            printo(output)

        return output

    def _run_clean(self, check_name, r):
        def cleanup():
            sys.stdout = old_stdout
            root.removeHandler(ch)
            output = my_stdout.getvalue()
            my_stdout.close()
            return output

        try:
            clean_func = self._clean_by_name(check_name)
        except CommandError:
            return
        old_stdout = sys.stdout
        sys.stdout = my_stdout = StringIO()
        root = logging.getLogger()
        ch = logging.StreamHandler(my_stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        root.addHandler(ch)
        success = True
        start = time.time()
        try:
            clean_func(r)
        except (Exception, NotFound) as e:
            cleanup()
            output = text_type(e)
            success = False
        else:
            output = cleanup()
        end = time.time()
        if utils.JSON_OUTPUT:
            clean_status = {
                "type": clean_func.__name__.split('_')[0],
                "name": clean_func.__name__,
                "output": output,
                "success": success,
                "duration": "%0.2f ms" % ((end - start) * 1000.0)
            }
            printo(json.dumps(clean_status))
        elif output:
            printo(output)

    def run(self, checks, clean):
        g = self.get_traversal()
        utils.log('Running...')
        start = time.time()
        for check_name in checks:
            r = self._run_check(check_name, g)
            if len(r) > 0 and clean is True:
                self._run_clean(check_name, r)
        end = time.time() - start
        utils.log('Run done in %ss' % end)
