package protodef

import (
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/types/descriptorpb"
)

func FindProtoFiles(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !f.IsDir() && strings.HasSuffix(strings.ToLower(f.Name()), ".proto") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

type MapFileDescriptorProto map[string]*descriptorpb.FileDescriptorProto

func GetFileDescriptors(root string) (MapFileDescriptorProto, error) {
	var imports []string

	files, err := FindProtoFiles(root)
	if err != nil {
		log.Fatal("findProtoFiles", err)
		return nil, err
	}

	resolved, err := protoparse.ResolveFilenames(imports, files...)
	if err != nil {
		log.Fatal("ResolveFilenames", err)
		return nil, err
	}

	fd, err := protoparse.Parser{}.ParseFiles(resolved...)
	if err != nil {
		log.Fatal("ParseFiles", err)
		return nil, err
	}

	fdmap := make(MapFileDescriptorProto, len(fd))
	for i, name := range resolved {
		basename := path.Base(name)
		fdmap[basename] = fd[i].AsFileDescriptorProto()
	}

	return fdmap, err
}
