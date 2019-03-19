import time

from gremlin_python.process.graph_traversal import __, union, select
from gremlin_python.process.traversal import within, gt
from gremlin_python import statics

from contrail_api_cli.utils import printo
from contrail_api_cli.exceptions import ResourceNotFound

from .utils import to_resources, log_resources, v_to_r, cmd, updated_five_min_ago
from . import utils


statics.default_lambda_language = 'gremlin-groovy'
statics.load_statics(globals())


def test_iip_without_vmi(g):
    g.addV('instance_ip').property('updated', (int(time.time()) - 10 * 60)).iterate()
    assert 1 == len(check_iip_without_vmi(g))


@log_resources
@to_resources
@updated_five_min_ago
def check_iip_without_vmi(g):
    """instance-ip without any virtual-machine-interface
    """
    return g.V().hasLabel('instance_ip').not_(
        __.out().hasLabel('virtual_machine_interface'))


def clean_iip_without_vmi(iips):
    for iip in iips:
        try:
            iip.delete()
            printo('Deleted %s' % iip)
        except ResourceNotFound:
            continue


@log_resources
@to_resources
@updated_five_min_ago
def check_unused_rt(g):
    """unused route-target
    """
    return g.V().hasLabel("route_target").not_(
        __.in_().hasLabel(within("routing_instance", "logical_router"))
    )


def clean_unused_rt(rts):
    cmd('clean-route-target')(paths=[rt.path for rt in rts],
                              zk_server=utils.ZK_SERVER,
                              exclude=[])


@log_resources
@to_resources
def check_iip_without_instance_ip_address(g):
    """instance-ip without any instance_ip_address property
    """
    return g.V().hasLabel("instance_ip").not_(
        __.has("instance_ip_address")
    )


def clean_iip_without_instance_ip_address(iips):
    for iip in iips:
        if not iip.fetch().refs.virtual_machine_interface:
            try:
                iip.delete()
                printo('Deleted %s' % iip)
            except ResourceNotFound:
                continue
            return
        vmi_vm = False
        for vmi in iip.refs.virtual_machine_interface:
            if vmi.fetch().refs.virtual_machine:
                vmi_vm = True
        if vmi_vm is False:
            try:
                iip.delete()
                printo('Deleted %s' % iip)
            except ResourceNotFound:
                pass
            try:
                vmi.delete()
                printo('Deleted %s' % vmi)
            except ResourceNotFound:
                pass


@log_resources
@to_resources
@updated_five_min_ago
def check_snat_without_lr(g):
    """Snat SI without any logical-router
    """
    return g.V().hasLabel("service_template").has("display_name", "netns-snat-template") \
        .in_().hasLabel("service_instance").not_(__.in_().hasLabel("logical_router"))


def clean_snat_without_lr(sis):
    cmd('clean-stale-si')(paths=[si.path for si in sis])


@log_resources
@to_resources
@updated_five_min_ago
def check_lbaas_without_lbpool(g):
    """LBaaS SI without any loadbalancer-pool
    """
    return g.V().hasLabel("service_template") \
        .has("display_name", "haproxy-loadbalancer-template") \
        .in_().hasLabel("service_instance") \
        .not_(__.in_().hasLabel("loadbalancer_pool"))


def clean_lbaas_without_lbpool(sis):
    cmd('clean-stale-si')(paths=[si.path for si in sis])


@log_resources
@to_resources
@updated_five_min_ago
def check_lbaas_without_vip(g):
    """LBaaS SI without any virtual-ip
    """
    return g.V().hasLabel("service_instance") \
        .where(__.in_().hasLabel("loadbalancer_pool").not_(__.in_().hasLabel("virtual_ip")))


def clean_lbaas_without_vip(sis):
    cmd('clean-stale-si')(paths=[si.path for si in sis])


def test_fip_pool_with_broken_fip(g):
    g.addV('floating_ip_pool').as_('fp') \
     .addV('floating_ip').property('_missing', True).property('fq_name', ['_missing']).property('updated', 0) \
     .addE('ref').to('fp') \
     .iterate()
    assert 1 == len(check_fip_pool_with_broken_fip(g))


@log_resources
@to_resources
@updated_five_min_ago
def check_fip_pool_with_broken_fip(g):
    """floating-ip-pool that has a floating-ip that does not exist (that crashes schema)
    """
    return g.V().hasLabel('floating_ip_pool').in_().hasLabel('floating_ip').has('_missing')


@log_resources
@to_resources
@updated_five_min_ago
def check_fip_without_parent(g):
    """floating-ip without parent link (that crashes schema)
    """
    return g.V().hasLabel("floating_ip").hasNot("parent_uuid")


@log_resources
@to_resources
@updated_five_min_ago
def check_ri_without_rt(g):
    """routing-instance that doesn't have any route-target (that crashes schema)
    """
    return g.V().hasLabel("routing_instance") \
        .not_(__.has('fq_name', within(["default-domain", "default-project", "ip-fabric", "__default__"],
                                       ["default-domain", "default-project", "__link_local__", "__link_local__"]))) \
        .not_(__.out().hasLabel("route_target"))


def clean_ri_without_rt(ris):
    cmd('fix-ri')(paths=[ri.path for ri in ris],
                  zk_server=utils.ZK_SERVER)


@log_resources
@to_resources
@updated_five_min_ago
def check_ri_without_vn(g):
    """routing-instance that doesn't have any virtual-network
    """
    return g.V().hasLabel('routing_instance').where(
        __.in_('parent').hasNot('fq_name')
    )


def clean_ri_without_vn(ris):
    # This will leave RTs, but check_unused_rt will remove
    # them later
    for ri in ris:
        try:
            ri.delete()
            printo('Deleted %s' % ri)
        except ResourceNotFound:
            pass


@log_resources
@to_resources
@updated_five_min_ago
def check_acl_without_sg(g):
    """access-control-list without security-group
    """
    return g.V().hasLabel('access_control_list').where(
        __.in_().hasNot('fq_name')
    )


def clean_acl_without_sg(acls):
    for acl in acls:
        try:
            acl.delete()
            printo('Deleted %s' % acl)
        except ResourceNotFound:
            continue


def check_duplicate_ip_addresses(g):
    """networks with duplicate ip addresses
    """
    r = g.V().hasLabel("virtual_network").as_('vn').flatMap(
        union(
            select('vn'),
            __.in_().hasLabel("instance_ip").has("instance_ip_address")
            .group().by("instance_ip_address").unfold()
            .filter(lambda: "it.get().value.size() > 1")
        ).fold().filter(lambda: "it.get().size() > 1")
    ).toList()
    if len(r) > 0:
        printo('Found %d %s:' % (len(r), check_duplicate_ip_addresses.__doc__.strip()))
    for dup in r:
        # FIXME:
        dup[0].label = 'virtual_network'
        # First item is the vn
        r_ = v_to_r(dup[0])
        printo('  - %s/%s - %s' % (r_.type, r_.uuid, r_.fq_name))
        for ips in dup[1:]:
            for ip, iips in ips.items():
                printo("      %s:" % ip)
                for iip in iips:
                    r_ = v_to_r(iip)
                    printo('        - %s/%s - %s' % (r_.type, r_.uuid, r_.fq_name))
    return r


def check_duplicate_default_sg(g):
    """duplicate default security groups
    """
    r = g.V().hasLabel('project').flatMap(
        __.out().hasLabel('security_group').has('display_name', 'default').group().by(
            __.in_().hasLabel('project').id()
        ).unfold()
        .filter(lambda: "it.get().value.size() > 1")
    ).toList()
    if len(r) > 0:
        printo('Found %d %s:' % (len(r), check_duplicate_default_sg.__doc__.strip()))
    projects = []
    for dup in r:
        for p, sgs in dup.items():
            projects.append(v_to_r(p))
            printo('  - %s/%s - %s' % (projects[-1].type, projects[-1].uuid, projects[-1].fq_name))
            for sg in sgs:
                r_ = v_to_r(sg)
                printo('    - %s/%s - %s' % (r_.type, r_.uuid, r_.fq_name))
    return projects


def clean_duplicate_default_sg(projects):
    cmd('fix-sg')(paths=[p.path for p in projects], yes=True)


def check_duplicate_public_ips(g):
    """duplicate public ips
    """
    r = g.V().hasLabel(within('floating_ip', 'instance_ip')) \
        .or_(__.has('floating_ip_address'), __.has('instance_ip_address')) \
        .property('ip_address', __.values('floating_ip_address', 'instance_ip_address')) \
        .group().by('ip_address').unfold() \
        .filter(lambda: "it.get().value.size() > 1 && it.get().value.findAll{it.label.value == 'floating_ip'} != []") \
        .toList()
    if len(r) > 0:
        printo('Found %d %s:' % (len(r), check_duplicate_public_ips.__doc__.strip()))
    return r


@log_resources
@to_resources
@updated_five_min_ago
def check_vn_without_ri(g):
    """virtual-network without any routing-instance
    """
    return g.V().hasLabel('virtual_network').not_(
        __.in_().hasLabel('routing_instance')
    )


@log_resources
@to_resources
@updated_five_min_ago
def check_vmi_without_ri(g):
    """virtual-machine-interface without any routing-instance
    """
    return g.V().hasLabel('virtual_machine_interface').not_(
        __.out().hasLabel('routing_instance')
    )


def test_rt_multiple_projects(g):
    g.addV('global_system_config').property('autonomous_system', 64512) \
     .addV('project').property('fq_name', ['p1']).as_('p1') \
     .addV('project').property('fq_name', ['p2']).as_('p2') \
     .addV('virtual_network').as_('vn1') \
     .addE('parent').to('p1') \
     .addV('virtual_network').as_('vn2') \
     .addE('parent').to('p2') \
     .addV('routing_instance').as_('ri1') \
     .addE('parent').to('vn1') \
     .addV('routing_instance').as_('ri2') \
     .addE('parent').to('vn2') \
     .addV('route_target').property('display_name', 'target:64512:1').as_('rt') \
     .addE('ref').from_('ri1') \
     .select('rt').addE('ref').from_('ri2') \
     .iterate()
    assert 1 == len(check_rt_multiple_projects(g))


def check_rt_multiple_projects(g):
    """route-target belonging to several tenants
    """
    asNumber = g.V().hasLabel('global_system_config').values('autonomous_system').next()
    rtPattern = "target:%d:.*" % asNumber
    r = g.V().hasLabel("route_target") \
             .has('display_name') \
             .filter(lambda: "it.get().value('display_name').matches('%s')" % rtPattern) \
             .where(
                 __.in_().hasLabel("routing_instance").out().hasLabel("virtual_network").out().hasLabel("project").dedup().count().is_(gt(1))
             ) \
             .project('rt_id', 'projects') \
             .by(__.id()) \
             .by(__.in_().hasLabel("routing_instance").out().hasLabel("virtual_network").out().hasLabel("project").dedup().project('id', 'fq_name').by(__.id()).by(__.values('fq_name')).fold()) \
             .toList()
    if len(r) > 0:
        printo('Found %d %s:' % (len(r), check_rt_multiple_projects.__doc__.strip()))
    for dups in r:
        printo('  route-target/%s' % dups['rt_id'])
        for p in dups['projects']:
            printo('    - project/%s - %s' % (p['id'], ":".join(p['fq_name'])))
    return r
