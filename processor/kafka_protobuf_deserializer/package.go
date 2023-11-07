package kafka_protobuf_deserializer

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

const (
	fieldImportPaths  = "import_paths"
	fieldKeyMessage   = "key_message"
	fieldValueMessage = "value_message"
	defaultKeyMessage = "PK"
	metaKafkaKey      = "kafka_key"
)

func init() {
	configSpec := service.NewConfigSpec().
		Field(service.NewStringListField(fieldImportPaths).
			Description("A list of directories containing .proto files, including all definitions required for parsing the target message. If left empty the current directory is used. Each directory listed will be walked with all found .proto files imported.").
			Default([]string{})).
		Field(service.NewStringField(fieldValueMessage).
			Description("Name of the protobuf Message for the kafka value")).
		Field(service.NewStringField(fieldKeyMessage).
			Description("Name of the protobuf Message for the kafka key").
			Default(defaultKeyMessage)).
		Summary("Deserializes data with the specified key and value FQN protobuf messages")

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (proc service.Processor, err error) {
		var valueMessageName string
		var keyMessageName string
		var includeKafkaMetadata bool

		var importPaths []string
		if importPaths, err = conf.FieldStringList(fieldImportPaths); err != nil {
			return nil, err
		}
		if valueMessageName, err = conf.FieldString(fieldValueMessage); err != nil {
			return nil, err
		}
		if keyMessageName, err = conf.FieldString(fieldKeyMessage); err != nil {
			return nil, err
		}

		var valueMessage protoreflect.MessageType
		var keyMessage protoreflect.MessageType
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
			includeKafkaMetadata: includeKafkaMetadata,
			logger:               mgr.Logger(),
			keyMessageDesc:       keyMessage.Descriptor(),
			valMessageDesc:       valueMessage.Descriptor(),
		}
		return
	}

	err := service.RegisterProcessor("kafka_protobuf_deserializer", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

// ------------------------------------------------------------------------------
type deserializerProcessor struct {
	includeKafkaMetadata bool
	logger               *service.Logger
	keyMessageDesc       protoreflect.MessageDescriptor
	valMessageDesc       protoreflect.MessageDescriptor
}

func (p *deserializerProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	// Read kafka key from metadata, skip if not available
	keyContent, found := m.MetaGet(metaKafkaKey)
	if !found {
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
	m.MetaSet(metaKafkaKey, string(jsonData))

	return []*service.Message{m}, nil
}

func (p *deserializerProcessor) Close(ctx context.Context) error {
	return nil
}
