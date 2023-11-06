package sink_formatter

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"strings"

	"github.com/benthosdev/benthos/v4/public/service"
)

type jsonMap map[string]interface{}

func init() {
	configSpec := service.NewConfigSpec()

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newFormatter(mgr.Logger(), mgr.Metrics()), nil
	}

	err := service.RegisterProcessor("sink_formatter", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type formatterProcessor struct {
	logger        *service.Logger
	countMessages *service.MetricCounter
}

func newFormatter(logger *service.Logger, metrics *service.Metrics) *formatterProcessor {
	return &formatterProcessor{
		logger:        logger,
		countMessages: metrics.NewCounter("messages"),
	}
}

func (r *formatterProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	// Reads the message being processed as struct, on error skip
	content, err := m.AsStructured()
	if err != nil {
		return nil, err
	}
	sKey, err := getKey(m, "kafka_key")
	if err != nil {
		return nil, err
	}

	// Creates new struct
	oObj := make(jsonMap)
	oObj["key"] = sKey
	oObj["value"] = content
	oObj["metadata"] = createMetadata(m)

	// Encode as JSON and pass the bytes
	// TODO: m.SetStructured changes "value" to non-obj on tombstones
	newBytes, err := json.Marshal(oObj)
	if err != nil {
		return nil, err
	}
	m.SetBytes(newBytes)

	r.countMessages.Incr(1)
	return []*service.Message{m}, nil
}

// Decodes kafka key, fails on missing meta field
func getKey(m *service.Message, meta_field string) (jsonMap, error) {
	oKey := make(jsonMap)
	sKey, ok := m.MetaGet(meta_field)
	if ok {
		json.Unmarshal([]byte(sKey), &oKey)
		return oKey, nil
	} else {
		return nil, errors.New("invalid message, without a key")
	}
}

// Creates structure for metadata field
func createMetadata(m *service.Message) jsonMap {
	oMeta := make(jsonMap)
	oMeta["offset"] = getMetaAsInt(m, "kafka_offset")
	oMeta["partition"] = getMetaAsInt(m, "partition")
	oMeta["timestamp"] = getMetaAsInt(m, "kafka_timestamp_unix")
	oMeta["is_tombstone"] = isTombstone(m)
	headers, ok := getCustomHeaders(m)
	if ok {
		oMeta["headers"] = headers
	}
	return oMeta
}

// Identify if the message represents a tombstone (key!=NULL, value=NULL)
func isTombstone(m *service.Message) bool {
	bytes, err := m.AsBytes()
	if err != nil {
		return false
	}
	// TODO: Find a better test
	return (len(bytes) <= 2)
}

// Read meta field as Integer value (offset, timestamp, partition)
func getMetaAsInt(m *service.Message, key string) int {
	val, ok := m.MetaGet(key)
	if ok {
		intval, err := strconv.Atoi(val)
		if err != nil {
			log.Printf("cannot parse meta value")
			return 0
		}
		return intval
	}
	return 0
}

// Walks all message meta fields, and gets only those not prefixed by "kafka_"
func getCustomHeaders(m *service.Message) (map[string]string, bool) {
	headers := make(map[string]string)
	m.MetaWalk(func(key string, val string) error {
		if !strings.HasPrefix(key, "kafka_") {
			headers[key] = val
		}
		return nil
	})
	if len(headers) > 0 {
		return headers, true
	}
	return nil, false
}

func (r *formatterProcessor) Close(ctx context.Context) error {
	return nil
}
