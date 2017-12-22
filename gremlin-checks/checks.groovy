graphFilename = args[0]
graphFilename = graphFilename.replaceFirst("^~", System.getProperty("user.home"))
if (! new File(graphFilename).isAbsolute()) {
   graphFilename = System.getProperty("user.working_dir") + "/" +  graphFilename
}
printf("Loading the graphson file '%s'...\n", graphFilename)

configuration = new org.apache.commons.configuration.BaseConfiguration()
configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, "UUID")
configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, graphFilename)
configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "graphson")

graph = TinkerGraph.open(configuration);
g = graph.traversal()

// To evaluate it only one time
asNumber = g.V().hasLabel('global_system_config').values('autonomous_system').next()

// A Helper to pretty print nodes
GraphTraversal.metaClass.show = { delegate.map{
  vertex = it.get()
  printf("%s/%s\n", vertex.label().replaceAll("_","-"), vertex.id());
  vertex.properties().each{ printf("  %-40s %s\n", it.key(), it.value())};
  println ""
  println "  back_refs"
  g.V(vertex.id).in("ref").each{ printf("    %-40s %s\n", it.label, it.id)}
  println();
  println "  refs"
  g.V(vertex.id).out("ref").each{ printf("    %-40s %s\n", it.label, it.id)}
  println();
  println "  parent"
  g.V(vertex.id).out("parent").each{ printf("    %-40s %s\n", it.label, it.id)}
  println();
  println "  children"
  g.V(vertex.id).in("parent").each{ printf("    %-40s %s\n", it.label, it.id)}
  println();
  }
}

def check(desc, expr) {
  println desc
  expr.each {
    println '  ' + it.label() + '/' + it.id()
    println '    (' + it.value('fq_name').join(":") + ')'
  }
  println ''
}

def checkMap(desc, expr) {
  println desc
  expr.each {
    println '  ' + it.key
    it.value.each {
      println '    ' + it.label() + '/' + it.id()
      println '      (' + it.value('fq_name').join(":") + ')'
    }
  }
  println ''
}

def checkListMap(desc, expr) {
  println desc
  expr.each{
    println '  ' + it[0].label() + '/' + it[0].id()
    it[1, it.size].each {
      if (it != null) {
        println '    ' + it.key
        it.value.each {
          println '      ' + it.label() + '/' + it.id()
          println '        (' + it.value('fq_name').join(":") + ')'
        }
      }
    }
  }
  println ''
}

println 'broken references'
g.V().hasNot('_missing').both().has('_missing').path().map(unfold().map(union(label(), id()).fold()).fold()).each{
    println '  ' + it[0][0] + '/' + it[0][1] + ' -> ' + it[1][0] + '/' + it[1][1]
}
println ''

check("virtual-network with instance-ip but without any virtual-machine-interface",
    g.V().hasLabel("virtual_network").not(__.in().hasLabel('virtual_machine_interface')).where(__.in().hasLabel("instance_ip"))
)

check("virtual-network without routing-instance",
    g.V().hasLabel('virtual_network').not(__.in().hasLabel('routing_instance'))
)

check("virtual-machine-interface without routing-instance",
    g.V().hasLabel("virtual_machine_interface").not(__.out().hasLabel("routing_instance"))
)

check("stale route-targets",
    g.V().hasLabel("route_target").not(__.in().hasLabel(within("routing_instance", "logical_router")))
)

check("instance-ip without any instance_ip_address",
    g.V().hasLabel("instance_ip").not(has("instance_ip_address"))
)

check("snat without any logical-router",
    g.V().hasLabel("service_template").has("display_name", "netns-snat-template").in().hasLabel("service_instance").not(__.in().hasLabel("logical_router"))
)

check("lbaas without any loadbalancer-pool",
    g.V().hasLabel("service_template").has("display_name", "haproxy-loadbalancer-template").in().hasLabel("service_instance").not(__.in().hasLabel("loadbalancer_pool"))
)

check("lbaas without any virtual-ip",
    g.V().hasLabel("service_instance").where(__.in().hasLabel("loadbalancer_pool").not(__.in().hasLabel("virtual_ip")))
)

check("routing-instance that doesn't have any route-target (that crashes schema)",
    g.V().hasLabel("routing_instance").not(has('fq_name', within(["default-domain", "default-project", "ip-fabric", "__default__"], ["default-domain", "default-project", "__link_local__", "__link_local__"]))).not(out().hasLabel("route_target"))
)

check("stale access-control-lists",
    g.V().hasLabel('access_control_list').where(__.outE().hasLabel('parent').count().is(eq(0)))
)

checkListMap("virtual-networks duplicate ips",
    g.V().hasLabel("virtual_network").as('vn').map(union(select('vn'), __.in().hasLabel("instance_ip").has("instance_ip_address").group().by("instance_ip_address").unfold().filter{it.get().value.size > 1}).fold()).filter{it.get().size > 1}
)

checkMap("duplicate floating-ips",
    g.V().hasLabel(within('floating_ip', 'instance_ip')).or(__.has('floating_ip_address'), __.has('instance_ip_address')).property('ip_address', values('floating_ip_address', 'instance_ip_address')).group().by('ip_address').unfold().filter{it.get().value.size > 1 && it.get().value.findAll{it.label() == "floating_ip"} != []}
)

checkMap("duplicate default security-groups",
    g.V().hasLabel('project').flatMap(__.in('parent').hasLabel('security_group').has('display_name', 'default').group().by(__.out('parent').hasLabel('project').id()).unfold().filter{it.get().value.size > 1})
)

rtPattern = "target:" + asNumber + ".*"
check("route target belonging to several tenants (only RT matching the pattern '" + rtPattern + "')",
    g.V().hasLabel("route_target").where(__.in().hasLabel("routing_instance").out().hasLabel("virtual_network").out().hasLabel("project").dedup().count().is(gt(1))).filter{it.get().value("display_name").matches(rtPattern)}
)
