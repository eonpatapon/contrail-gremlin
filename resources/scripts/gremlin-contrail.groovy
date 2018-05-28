def globals = [:]

globals << [all : graph.traversal(), g : graph.traversal().withStrategies(SubgraphStrategy.build().vertexProperties(hasNot('_missing').hasNot('_incomplete')).create())]
