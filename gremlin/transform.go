package gremlin

func TransformVertex(v Vertex) (Vertex, error) {
	switch v.Label {
	default:
		return v, nil
	}
}
