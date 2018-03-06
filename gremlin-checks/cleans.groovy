// This script generates a list of resources that can be removed. Each
// lines contains several resources that have to be deleted
// sequentially while resources of different lines can be deleted in
// parallel.
//
// Usage: gremlin-console -i cleans.groovy ...

groovy.util.CliBuilder 

cli = new CliBuilder(usage:'cleans.groovy')

cli.with {
    // This disabled project list can be generated with
    // keystone tenant-list  | awk '$6 == "False" { print $2 }'
    d (longOpt: 'disabled', args:1, required:true, "The project disabled file path. Each line is a project id.")
    g (longOpt: 'graph', args:1, required:true, 'The graph json file path')
}

opts = cli.parse(args)
if (!opts) {
	System.exit(1)
}

// Load the disabled projects file into an array
disabledFilename = opts.d
disabledFilename = disabledFilename.replaceFirst("^~", System.getProperty("user.home"))
if (! new File(disabledFilename).isAbsolute()) {
   disabledFilename = System.getProperty("user.working_dir") + "/" +  disabledFilename
}
disabled = new File(disabledFilename).readLines()

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


// Get projects that have been disabled
p = g.V().hasLabel("project").filter{disabled.contains(it.get().id().toString().replace('-',''))}.as("p");
// Filter on project that only have a security group neighbor.
t = p.not(where(__.in().not(hasLabel("security_group")))).in().as("sg").select("sg", "p")
t.each{
    printf("security-group/%s project/%s\n", it["sg"].id(), it["p"].id())
}

