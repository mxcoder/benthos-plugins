package bigquery_writeapi

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/mxcoder/benthos-plugin/utils/protobuf"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	fieldBatching      = "batching"
	fieldMaxInFlight   = "max_in_flight"
	fieldProject       = "project"
	fieldDataset       = "dataset"
	fieldTable         = "table"
	fieldImportPaths   = "import_paths"
	fieldMessage       = "message"
	fieldSerialize     = "serialize"
	defaultMaxInFlight = 1
	outputName         = "gcp_bigquery_writeapi"
	maxWriteBatchSize  = 8388608 // 8MB
)

func init() {
	configSpec := service.NewConfigSpec().
		Summary("Writes to BigQuery using WriteAPI").
		Field(service.NewBatchPolicyField(fieldBatching)).
		Field(service.NewIntField(fieldMaxInFlight).
			Description("Max in-flight operations").
			Default(defaultMaxInFlight)).
		Field(service.NewStringField(fieldProject).
			Description("GCP Project name")).
		Field(service.NewStringField(fieldDataset).
			Description("BigQuery dataset name")).
		Field(service.NewStringField(fieldTable).
			Description("BigQuery table name")).
		Field(service.NewBoolField(fieldSerialize).
			Description("Toggles data serialization").
			Default(false)).
		Field(service.NewStringListField(fieldImportPaths).
			Description("A list of directories containing .proto files, including all definitions required for parsing the target message. If left empty the current directory is used. Each directory listed will be walked with all found .proto files imported.").
			Default([]string{})).
		Field(service.NewStringField(fieldMessage).
			Description("Protobuf message name").
			Default(""))

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (
		out service.BatchOutput, policy service.BatchPolicy, maxInFlight int, err error) {
		var project string
		var dataset string
		var table string

		logger := mgr.Logger().With(map[string]string{"output": outputName})
		if maxInFlight, err = conf.FieldInt(fieldMaxInFlight); err != nil {
			return
		}
		if policy, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
			return
		}
		if policy.ByteSize == 0 || policy.ByteSize > maxWriteBatchSize {
			logger.Warnf("batching.byte_size default value is %d bytes", maxWriteBatchSize)
			policy.ByteSize = maxWriteBatchSize
		}
		if project, err = conf.FieldString(fieldProject); err != nil {
			return
		}
		if dataset, err = conf.FieldString(fieldDataset); err != nil {
			return
		}
		if table, err = conf.FieldString(fieldTable); err != nil {
			return
		}

		var protobufSerialize bool
		var protobufMessageName string
		var importPaths []string
		if importPaths, err = conf.FieldStringList(fieldImportPaths); err != nil {
			return nil, policy, maxInFlight, err
		}
		if protobufMessageName, err = conf.FieldString(fieldMessage); err != nil {
			return nil, policy, maxInFlight, err
		}
		if protobufSerialize, err = conf.FieldBool(fieldSerialize); err != nil {
			return nil, policy, maxInFlight, err
		}

		_, protobufTypes, err := protobuf.LoadDescriptors(mgr.FS(), importPaths)
		if err != nil {
			return nil, policy, maxInFlight, fmt.Errorf("unable to load protobuf Descriptors from: %v", importPaths)
		}
		protobufMessageType, err := protobuf.LoadMessage(protobufTypes, protobufMessageName)
		if err != nil {
			return nil, policy, maxInFlight, fmt.Errorf("unable to load protobuf Message: %v", protobufMessageName)
		}
		normalizedDescriptor, err := adapt.NormalizeDescriptor(protobufMessageType.Descriptor())
		if err != nil {
			return nil, policy, maxInFlight, fmt.Errorf("unable to normalize protobuf Message: %v", protobufMessageName)
		}

		out = &bqWriter{
			log:                  logger,
			project:              project,
			dataset:              dataset,
			table:                table,
			protobufTypes:        protobufTypes,
			protobufMessage:      protobufMessageType,
			protobufSerialize:    protobufSerialize,
			normalizedDescriptor: normalizedDescriptor,
		}
		return
	}

	err := service.RegisterBatchOutput(outputName, configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

// ------------------------------------------------------------------------------
type bqWriter struct {
	log                  *service.Logger
	project              string
	dataset              string
	table                string
	connMut              sync.RWMutex
	client               *managedwriter.Client
	stream               *managedwriter.ManagedStream
	protobufTypes        *protoregistry.Types
	protobufMessage      protoreflect.MessageType
	protobufSerialize    bool
	normalizedDescriptor *descriptorpb.DescriptorProto
}

func (b *bqWriter) Connect(ctx context.Context) (err error) {
	b.connMut.Lock()
	defer b.connMut.Unlock()

	var client *managedwriter.Client
	var stream *managedwriter.ManagedStream
	defer func() {
		if err != nil {
			if b.stream != nil {
				b.stream.Close()
			}
			if b.client != nil {
				b.client.Close()
			}
		}
	}()

	// Instantiate a managedwriter client to handle interactions with the service.
	client, err = managedwriter.NewClient(ctx, b.project, managedwriter.WithMultiplexing())
	if err != nil {
		return fmt.Errorf("managedwriter.NewClient: %w", err)
	}
	b.log.Infof("Created ManagedWriter client")

	tableReference := managedwriter.TableParentFromParts(b.project, b.dataset, b.table)
	b.log.Infof("Table from parts: %v:%v:%v\n", b.project, b.dataset, b.table)

	// declare managed stream
	stream, err = client.NewManagedStream(
		ctx,
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithDestinationTable(tableReference),
		managedwriter.WithSchemaDescriptor(b.normalizedDescriptor),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		return fmt.Errorf("NewManagedStream: %w", err)
	}
	b.log.Infof("Created ManagedStream %v", stream.StreamName())
	b.client = client
	b.stream = stream

	return nil
}

func (b *bqWriter) WriteBatch(ctx context.Context, msgs service.MessageBatch) (err error) {
	/* ------------------------------ Encoding data ----------------------------- */
	var row []byte
	var rows = make([][]byte, 0, len(msgs))
	b.log.Infof("Received %v messages", len(msgs))
	for _, msg := range msgs {
		if b.protobufSerialize {
			if row, err = messageToProtobuf(msg, b.protobufMessage, b.protobufTypes); err != nil {
				return fmt.Errorf("error serializing message: %w", err)
			}
		} else {
			if row, err = msg.AsBytes(); err != nil {
				return fmt.Errorf("error passing message bytes: %w", err)
			}
		}
		rows = append(rows, row)
	}
	/* ------------------------------ Writing data ------------------------------ */
	var result *managedwriter.AppendResult
	b.connMut.Lock()
	b.log.Infof("Appending %v rows", len(rows))
	result, err = b.stream.AppendRows(ctx, rows)
	b.connMut.Unlock()
	if err != nil {
		return fmt.Errorf("AppendRows call error: %w", err)
	}
	/* ----------------------------- Checking result ---------------------------- */
	_, err = result.GetResult(ctx)
	if err != nil {
		if apiErr, ok := apierror.FromError(err); ok {
			// We now have an instance of APIError, which directly exposes more specific
			// details about multiple failure conditions include transport-level errors.
			storageErr := &storagepb.StorageError{}
			if e := apiErr.Details().ExtractProtoMessage(storageErr); e != nil {
				// storageErr now contains service-specific information about the error.
				b.log.Errorf("WriteAPI error code %s: %v", storageErr.GetCode().String(), apiErr)
			} else {
				b.log.Errorf("WriteAPI unknown error %v", storageErr.String())
			}
		}
	}
	return nil
}

func (b *bqWriter) Close(ctx context.Context) error {
	b.connMut.Lock()
	if b.stream != nil {
		b.log.Infof("Closing ManagedStream %v", b.stream.StreamName())
		b.stream.Close()
	}
	if b.client != nil {
		b.log.Infof("Closing ManagedWriter client")
		b.client.Close()
	}
	b.connMut.Unlock()
	return nil
}

func messageToProtobuf(part *service.Message, messageType protoreflect.MessageType, types *protoregistry.Types) ([]byte, error) {
	var err error
	var msgBytes []byte
	var protoBytes []byte
	if msgBytes, err = part.AsBytes(); err != nil {
		return nil, err
	}
	newMessage := dynamicpb.NewMessage(messageType.Descriptor())
	opts := protojson.UnmarshalOptions{Resolver: types}
	if err := opts.Unmarshal(msgBytes, newMessage); err != nil {
		return nil, fmt.Errorf("failed to load message into Protobuf schema: %w", err)
	}
	if protoBytes, err = proto.Marshal(newMessage); err != nil {
		return nil, fmt.Errorf("failed to serialize to protobuf message: %v", err)
	}
	return protoBytes, nil
}
