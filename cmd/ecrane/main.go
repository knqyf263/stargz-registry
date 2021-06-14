package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/containerd/stargz-snapshotter/estargz"
	"golang.org/x/sync/errgroup"

	"github.com/knqyf263/stargz-registry/remote"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	args := os.Args
	if len(args) != 3 {
		fmt.Println("Usage: ecrane IMAGE_NAME FILE_PATH")
		return nil
	}
	var (
		imageName = args[1]
		filePath  = args[2]
	)

	ctx := context.Background()

	r, err := remote.New(imageName)
	if err != nil {
		return err
	}

	layers, err := r.Layers(ctx)
	if err != nil {
		return err
	}

	var result sync.Map
	g, ctx := errgroup.WithContext(ctx)

	for _, layer := range layers {
		l := layer
		g.Go(func() error {
			sr := io.NewSectionReader(l, 0, l.Size())
			esgz, err := estargz.Open(sr)
			if err != nil {
				return err
			}

			if e, ok := esgz.Lookup(filePath); ok {
				sr, err = esgz.OpenFile(e.Name)
				if err != nil {
					return err
				}

				b, err := io.ReadAll(sr)
				if err != nil {
					return err
				}

				result.Store(layer.Digest(), b)
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	for i := len(layers) - 1; i >= 0; i-- {
		v, ok := result.Load(layers[i].Digest())
		if !ok {
			continue
		}
		fmt.Println(string(v.([]byte)))
	}

	return nil
}
