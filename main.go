package main

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/service"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/kafka"
	_ "github.com/benthosdev/benthos/v4/public/components/prometheus"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	_ "github.com/benthosdev/benthos/v4/public/components/pure/extended"

	// Import all standard Benthos components
	// _ "github.com/benthosdev/benthos/v4/public/components/all"

	// _ "github.com/mxcoder/benthos-plugin/output/bigquery_writeapi"
	_ "github.com/mxcoder/benthos-plugin/processor/kafka_protobuf_deserializer"
	// _ "github.com/mxcoder/benthos-plugin/processor/sink_formatter"
)

func main() {
	service.RunCLI(context.Background())
}
