package protobuf

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func RegistriesFromMap(filesMap map[string]string) (*protoregistry.Files, *protoregistry.Types, error) {
	var parser protoparse.Parser
	parser.Accessor = protoparse.FileContentsFromMap(filesMap)

	names := make([]string, 0, len(filesMap))
	for k := range filesMap {
		names = append(names, k)
	}
	fds, err := parser.ParseFiles(names...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to Parse protobuf files: %v", err)
	}

	files, types := &protoregistry.Files{}, &protoregistry.Types{}
	for _, v := range fds {
		if err := files.RegisterFile(v.UnwrapFile()); err != nil {
			return nil, nil, fmt.Errorf("failed to register file '%v': %w", v.GetName(), err)
		}
		for _, t := range v.GetMessageTypes() {
			if err := types.RegisterMessage(dynamicpb.NewMessageType(t.UnwrapMessage())); err != nil {
				return nil, nil, fmt.Errorf("failed to register type '%v': %w", t.GetFullyQualifiedName(), err)
			}
			for _, nt := range t.GetNestedMessageTypes() {
				if err := types.RegisterMessage(dynamicpb.NewMessageType(nt.UnwrapMessage())); err != nil {
					return nil, nil, fmt.Errorf("failed to register type '%v': %w", nt.GetFullyQualifiedName(), err)
				}
			}
		}
	}
	return files, types, nil
}

func LoadDescriptors(fileSystem fs.FS, importPaths []string) (*protoregistry.Files, *protoregistry.Types, error) {
	files := map[string]string{}
	for _, importPath := range importPaths {
		if err := fs.WalkDir(fileSystem, importPath, func(path string, info fs.DirEntry, ferr error) error {
			if ferr != nil {
				return fmt.Errorf("failed to walkDir: %v", ferr)
			}
			if filepath.Ext(info.Name()) == ".proto" {
				rPath, ferr := filepath.Rel(importPath, path)
				if ferr != nil {
					return fmt.Errorf("failed to get relative path: %v", ferr)
				}
				content, ferr := os.ReadFile(path)
				if ferr != nil {
					return fmt.Errorf("failed to read import %v: %v", path, ferr)
				}
				files[rPath] = string(content)
			}
			return nil
		}); err != nil {
			return nil, nil, fmt.Errorf("failed to get walk path: %v", err)
		}
	}
	return RegistriesFromMap(files)
}

func LoadMessage(types *protoregistry.Types, messageName string) (messageType protoreflect.MessageType, err error) {
	if messageName == "" {
		return nil, errors.New("messageName must not be empty")
	}
	types.RangeMessages(func(mt protoreflect.MessageType) bool { return true })
	if messageType, err = types.FindMessageByName(protoreflect.FullName(messageName)); err != nil {
		return nil, fmt.Errorf("unable to find message '%v'", messageName)
	}
	return messageType, err
}

func MessageToJSON(pb proto.Message) (out []byte, err error) {
	out, err = protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}.Marshal(pb)
	if err != nil {
		return []byte(""), fmt.Errorf("cant encode proto.Message (%v) to JSON '%v'", pb, err)
	}
	return
}
