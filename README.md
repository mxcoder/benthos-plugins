# Benthos plugins

## Processors

### KV Protobuf deserializer

```yaml
input:
  - kafka:
      addresses: ["${KAFKA}"]
      topics: ["${TOPIC}"]
      consumer_group: "${GROUP}"
    processors:
      - protobuf_deserializer:
          protodir: ./protos
          valueMessage: com.Entity
          keyMessage: com.Entity.PK
```

### Sink Formatter

```yaml
processors:
  - sink_formatter: {}
```

## Output

### BigQuery using Write API

```yaml
output:
  bqwrite:
    project: "${BQ_PROJECT}"
    dataset: "${BQ_DATASET}"
    table: "${BQ_TABLE}"
    protobuf_path: "./protos"
    protobuf_name: com.Entity
    batching:
      period: 10s
      byte_size: 10485760
```
