package protobuf_deserializer

import (
	"context"
	"errors"
	"fmt"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/mxcoder/benthos-plugin/utils/protobuf"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func init() {
	configSpec := service.NewConfigSpec().
		Summary("Deserializes data with the specified key and value FQN protobuf messages").
		Field(service.NewStringField("protobuf_path").
			Description("Directory of proto files").
			Default("./protos")).
		Field(service.NewStringField("value_message").
			Description("Name of the protobuf Message for the main payload")).
		Field(service.NewStringField("key_message").
			Description("Name of the protobuf Message for the kafka key").
			Default("PK")).
		Field(service.NewBoolField("clear_key").
			Description("Removes the key field from the resulting JSON").
			Default(false))

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (proc service.Processor, err error) {
		logger := mgr.Logger()
		var protobufPath string
		var valueMessageName string
		var keyMessageName string
		var clearKey bool
		if protobufPath, err = conf.FieldString("protobuf_path"); err != nil {
			return nil, err
		}
		if valueMessageName, err = conf.FieldString("value_message"); err != nil {
			return nil, err
		}
		if keyMessageName, err = conf.FieldString("key_message"); err != nil {
			return nil, err
		}
		if clearKey, err = conf.FieldBool("clear_key"); err != nil {
			return nil, err
		}

		var importPaths []string
		var valueMessage protoreflect.MessageType
		var keyMessage protoreflect.MessageType
		importPaths = append(importPaths, protobufPath)
		_, protobufTypes, err := protobuf.LoadDescriptors(mgr.FS(), importPaths)
		if err != nil {
			return nil, fmt.Errorf("unable to load protobuf Descriptors from: %v; %v", importPaths, err)
		}
		if valueMessage, err = protobuf.LoadMessage(protobufTypes, valueMessageName); err != nil {
			return nil, fmt.Errorf("unable to find value protobuf message: %v", valueMessageName)
		}
		if keyMessage, err = protobuf.LoadMessage(protobufTypes, keyMessageName); err != nil {
			return nil, fmt.Errorf("unable to find key protobuf message: %v", keyMessageName)
		}
		proc = &deserializerProcessor{
			logger:         logger,
			clearKey:       clearKey,
			valMessageDesc: valueMessage.Descriptor(),
			keyMessageDesc: keyMessage.Descriptor(),
		}
		return
	}

	err := service.RegisterProcessor("protobuf_deserializer", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

// ------------------------------------------------------------------------------
type deserializerProcessor struct {
	logger         *service.Logger
	clearKey       bool
	valMessageDesc protoreflect.MessageDescriptor
	keyMessageDesc protoreflect.MessageDescriptor
}

func (p *deserializerProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	// Read kafka key from metadata, skip if not available
	keyContent, keyFound := m.MetaGet("kafka_key")
	if !keyFound {
		return nil, errors.New("kafka message without key")
	}
	// Read main payload (value) bytes
	valueContent, err := m.AsBytes()
	if err != nil {
		return nil, err
	}

	// Create empty proto Messages from descriptors
	pbValMessage := dynamicpb.NewMessage(p.valMessageDesc)
	pbKeyMessage := dynamicpb.NewMessage(p.keyMessageDesc)

	// Deserialize contents into messages, skip on errors
	if proto.Unmarshal([]byte(keyContent), pbKeyMessage) != nil {
		return nil, errors.New("key deserialize error")
	}
	if proto.Unmarshal(valueContent, pbValMessage) != nil {
		return nil, errors.New("value deserialize error")
	}

	// Clear the key field from value
	if p.clearKey {
		pbValMessage.Clear(p.valMessageDesc.Fields().ByName("key"))
	}

	// Pass the bytes data as JSON
	var jsonData []byte
	if jsonData, err = protobuf.MessageToJSON(pbValMessage); err != nil {
		return nil, fmt.Errorf("unable to marshal message to JSON '%v'", err)
	}
	m.SetBytes(jsonData)

	// Set the kafka_key value to the deserialized value as JSON
	if jsonData, err = protobuf.MessageToJSON(pbKeyMessage); err != nil {
		return nil, fmt.Errorf("unable to marshal key message to JSON '%v'", err)
	}
	m.MetaSet("kafka_key", string(jsonData))

	return []*service.Message{m}, nil
}

func (p *deserializerProcessor) Close(ctx context.Context) error {
	return nil
}
