# DAG store
Project for fun.

### Configuration:
[config/services/graph/config.yaml](config/services/graph/config.yaml)

### API endpoints:
- POST /nodes
request body:
```
{
    "nodes": [
        {
            "id": "3",
            "parent": "1"
        }
    ]
}
```
  - GET /nodes
response:
```
[
    "3",
    "1"
]
```
  - GET /nodes/{node_id}/trees
request /nodes/3/trees
```
{
    "trees": [
        [
            "1",
            "3"
        ]
    ]
}
```
 
### Run service   
`python service.py graph`

_Default configuration binds to 127.0.0.1:8080_

### Benchmark jmx
[benchmark/benchmark.jmx](benchmark/benchmark.jmx)

### Tests
`rake dev:test`

`rake dev:lint`
