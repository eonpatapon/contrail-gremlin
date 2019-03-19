from __future__ import unicode_literals
import functools
from six import text_type, binary_type
import time

from gremlin_python.process.graph_traversal import id, label, union, values, coalesce, constant
from gremlin_python.process.traversal import lt

from contrail_api_cli.resource import Resource
from contrail_api_cli.utils import printo
from contrail_api_cli.manager import CommandManager
from contrail_api_cli.exceptions import CommandError


JSON_OUTPUT = False
ZK_SERVER = 'localhost:2181'


def log(string):
    if JSON_OUTPUT:
        return
    printo(string)


def updated_five_min_ago(fun):
    @functools.wraps(fun)
    def wrapper(*args):
        time_point = int(time.time()) - 5 * 60
        g = fun(*args)
        return g.has('updated', (binary_type('_t'), lt(time_point)))
    return wrapper


def to_resources(fun):
    @functools.wraps(fun)
    def wrapper(*args):
        t = fun(*args)
        r = t.map(union(label(), id(), coalesce(values('fq_name'), constant(''))).fold()).toList()
        # convert gremlin result in [Resource]
        resources = []
        for r_ in r:
            res_type = r_[0].replace('_', '-')
            uuid = text_type(r_[1])
            fq_name = r_[2]
            resources.append(Resource(res_type, uuid=uuid, fq_name=fq_name))
        return resources
    return wrapper


def log_resources(fun):
    @functools.wraps(fun)
    def wrapper(*args):
        r = fun(*args)
        if len(r) > 0 and not JSON_OUTPUT:
            printo('Found %d %s:' % (len(r), fun.__doc__.strip()))
            for r_ in r:
                printo('  - %s/%s - %s' % (r_.type, r_.uuid, r_.fq_name))
        return r
    return wrapper


def v_to_r(v):
    if v.label:
        return Resource(v.label.replace('_', '-'), uuid=text_type(v.id))
    raise CommandError('Vertex has no label, cannot transform it to Resource')


def cmd(name):
    return CommandManager().get(name)
