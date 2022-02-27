package protobuf_deserializer

import (
	"context"
	"errors"
	"log"

	"github.com/Jeffail/benthos/v3/public/service"

	"github.com/mxcoder/benthos-plugin/utils/protodef"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type protoMD protoreflect.MessageDescriptor

func init() {
	// Config spec is empty for now as we don't have any dynamic fields.
	configSpec := service.NewConfigSpec().
		Summary("Deserializes protobuf messages").
		Field(service.NewStringField("protodir").
			Description("Directory of proto files").
			Default("./protos")).
		Field(service.NewStringField("protofile").
			Description("Name of the proto file")).
		Field(service.NewStringField("valueMessage").
			Description("Name of the protobuf Message for the main payload")).
		Field(service.NewStringField("keyMessage").
			Description("Name of the protobuf Message for the kafka key").
			Default("PK"))

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		protodir, err := conf.FieldString("protodir")
		if err != nil {
			return nil, err
		}
		protoFileDescriptors, err := protodef.GetFileDescriptors(protodir)
		if err != nil {
			panic(err)
		}

		protofile, err := conf.FieldString("protofile")
		if err != nil {
			return nil, err
		}

		messageName, err := conf.FieldString("valueMessage")
		if err != nil {
			return nil, err
		}

		keyMessageName, err := conf.FieldString("keyMessage")
		if err != nil {
			return nil, err
		}

		fd, err := protodesc.NewFile(protoFileDescriptors[protofile], nil)
		if err != nil {
			log.Fatal("Unable to find protofile: " + protofile)
			return nil, err
		}
		vmd := fd.Messages().ByName(protoreflect.Name(messageName))
		if vmd == nil {
			log.Fatal("Unable to find message: " + messageName)
		}
		kmd := vmd.Messages().ByName(protoreflect.Name(keyMessageName))
		if kmd == nil {
			log.Fatal("Unable to find key message: " + keyMessageName)
		}

		return newDeserializerProcessor(mgr.Logger(), vmd, kmd), nil
	}

	err := service.RegisterProcessor("protobuf_deserializer", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type deserializerProcessor struct {
	logger         *service.Logger
	valMessageDesc protoMD
	keyMessageDesc protoMD
}

func newDeserializerProcessor(logger *service.Logger, vmd protoMD, kmd protoMD) *deserializerProcessor {
	return &deserializerProcessor{
		logger:         logger,
		valMessageDesc: vmd,
		keyMessageDesc: kmd,
	}
}

func (r *deserializerProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
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
	pbValMessage := dynamicpb.NewMessage(r.valMessageDesc)
	pbKeyMessage := dynamicpb.NewMessage(r.keyMessageDesc)

	// Deserialize contents into messages, skip on errors
	if proto.Unmarshal([]byte(keyContent), pbKeyMessage) != nil {
		return nil, errors.New("key deserialize error")
	}
	if proto.Unmarshal(valueContent, pbValMessage) != nil {
		return nil, errors.New("value deserialize error")
	}

	// Clear the key field from value
	pbValMessage.Clear(r.valMessageDesc.Fields().ByName("key"))

	// Pass the bytes data as JSON
	m.SetBytes(pbMessageToJSON(pbValMessage))
	m.MetaSet("kafka_key", string(pbMessageToJSON(pbKeyMessage)))

	return []*service.Message{m}, nil
}

func pbMessageToJSON(pb proto.Message) []byte {
	out, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}.Marshal(pb)
	if err != nil {
		log.Println("cant encode proto.Message to JSON", err)
		return []byte("")
	}
	return out
}

func (r *deserializerProcessor) Close(ctx context.Context) error {
	return nil
}
