import trafaret as t

NodesTrafaret = t.Dict(
    {
        t.Key('id', to_name='node_id'): t.String,
        t.Key('parent', optional=True): t.String,
    }
)


NodesTrafaret = t.Dict(
    nodes=t.List(NodesTrafaret)
)
