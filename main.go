package main

import (
	"context"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
	"github.com/Jeffail/benthos/v3/public/service"

	_ "github.com/mxcoder/benthos-plugin/processor/protobuf_deserializer"
	_ "github.com/mxcoder/benthos-plugin/processor/sink_formatter"
)

func main() {
	service.RunCLI(context.Background())
}
