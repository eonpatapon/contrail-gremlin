// Generate a map of custom RT added to VNs
//
// Usage: gremlin-console -i bgpvpn.groovy ...

groovy.util.CliBuilder

cli = new CliBuilder(usage:'bgpvpn.groovy')

cli.with {
    g (longOpt: 'graph', args:1, required:true, 'The graph json file path')
}

opts = cli.parse(args)
if (!opts) {
	System.exit(1)
}

// Load the graph json file
graphFilename = opts.g
graphFilename = graphFilename.replaceFirst("^~", System.getProperty("user.home"))
if (! new File(graphFilename).isAbsolute()) {
   graphFilename = System.getProperty("user.working_dir") + "/" +  graphFilename
}

configuration = new org.apache.commons.configuration.BaseConfiguration()
configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, "UUID")
configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, graphFilename)
configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "graphson")

graph = TinkerGraph.open(configuration);
g = graph.traversal()

asNumber = g.V().hasLabel('global_system_config').values('autonomous_system').next()
rtPattern = "target:" + asNumber + ":.*"

g.V().hasLabel("route_target").has('display_name').filter{!it.get().value("display_name").matches(rtPattern)}.project('rt', 'mode', 'assocs').by('display_name').by(__.inE().coalesce(values('import_export'), constant('import_export'))).by(__.in().hasLabel('routing_instance').out().hasLabel('virtual_network').group().by(__.out().hasLabel('project').id()).by(id)).sideEffect{ println groovy.json.JsonOutput.toJson(it.get()) }.iterate()
