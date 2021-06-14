package remote

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

type Remote struct {
	ref   name.Reference
	rt    http.RoundTripper
	image v1.Image
}

func New(s string) (Remote, error) {
	ref, err := name.ParseReference(s)
	if err != nil {
		return Remote{}, err
	}

	// Fetch credentials based on your docker config file, which is $HOME/.docker/config.json or $DOCKER_CONFIG.
	auth, err := authn.DefaultKeychain.Resolve(ref.Context())
	if err != nil {
		return Remote{}, err
	}

	// Construct an http.Client that is authorized to pull from gcr.io/google-containers/pause.
	scopes := []string{ref.Scope(transport.PullScope)}
	t, err := transport.New(ref.Context().Registry, auth, http.DefaultTransport, scopes)
	if err != nil {
		return Remote{}, err
	}

	img, err := remote.Image(ref, remote.WithTransport(t))
	if err != nil {
		return Remote{}, err
	}

	return Remote{
		ref:   ref,
		rt:    t,
		image: img,
	}, nil
}

func (r Remote) Layers(ctx context.Context) ([]*Layer, error) {
	layers, err := r.image.Layers()
	if err != nil {
		return nil, err
	}

	repoURL := url.URL{
		Scheme: r.ref.Context().Scheme(),
		Host:   r.ref.Context().RegistryStr(),
		Path:   fmt.Sprintf("/v2/%s/", r.ref.Context().RepositoryStr()),
	}

	var eLayers []*Layer
	for _, layer := range layers {
		size, err := layer.Size()
		if err != nil {
			return nil, err
		}

		digest, err := layer.Digest()
		if err != nil {
			return nil, err
		}

		// Get blob URL
		blobURL := repoURL
		blobURL.Path = path.Join(blobURL.Path, "blobs", digest.String())

		redirectedURL, err := redirect(ctx, blobURL.String(), r.rt, 30*time.Second)
		if err != nil {
			return nil, err
		}

		eLayers = append(eLayers, &Layer{
			digest:  digest,
			url:     redirectedURL,
			blobURL: blobURL.String(),
			size:    size,
			rt:      r.rt,
		})
	}

	return eLayers, nil
}

type Layer struct {
	digest  v1.Hash
	url     string
	blobURL string
	size    int64
	rt      http.RoundTripper
}

func (l *Layer) Digest() v1.Hash {
	return l.digest
}

func (l *Layer) Size() int64 {
	return l.size
}

// ReadAt reads remote chunks from specified offset for the buffer size.
func (l *Layer) ReadAt(p []byte, offset int64) (int, error) {
	if len(p) == 0 || offset > l.size {
		return 0, nil
	}

	// Read required data
	rc, err := l.fetch(context.Background(), offset, offset+int64(len(p))-1)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	return io.ReadFull(rc, p)
}

func (l *Layer) fetch(ctx context.Context, begin, end int64) (io.ReadCloser, error) {
	// Request to the registry
	req, err := http.NewRequestWithContext(ctx, "GET", l.url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", begin, end))
	req.Header.Add("Accept-Encoding", "identity")
	req.Close = false

	client := &http.Client{Transport: l.rt}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode == http.StatusOK {
		return res.Body, nil
	} else if res.StatusCode == http.StatusPartialContent {
		mediaType, _, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
		if err != nil {
			return nil, fmt.Errorf("invalid media type %q: %w", mediaType, err)
		}
		if strings.HasPrefix(mediaType, "multipart/") {
			return nil, fmt.Errorf("multipart not supported")
		}

		return res.Body, nil
	}

	return nil, fmt.Errorf("unexpected status code: %v", res.Status)
}

func redirect(ctx context.Context, blobURL string, tr http.RoundTripper, timeout time.Duration) (url string, err error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	// We use GET request for redirect.
	// gcr.io returns 200 on HEAD without Location header (2020).
	// ghcr.io returns 200 on HEAD without Location header (2020).
	req, err := http.NewRequestWithContext(ctx, "GET", blobURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to make request to the registry: %w", err)
	}
	req.Close = false
	req.Header.Set("Range", "bytes=0-1")
	res, err := tr.RoundTrip(req)
	if err != nil {
		return "", fmt.Errorf("failed to request: %w", err)
	}
	defer func() {
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}()

	if res.StatusCode/100 == 2 {
		url = blobURL
	} else if redir := res.Header.Get("Location"); redir != "" && res.StatusCode/100 == 3 {
		// TODO: Support nested redirection
		url = redir
	} else {
		return "", fmt.Errorf("failed to access to the registry with code %v", res.StatusCode)
	}

	return
}
