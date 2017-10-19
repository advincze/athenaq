package s3path

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

const scheme = "s3"

type S3Path struct {
	Bucket string
	Key    string
}

func (p *S3Path) Subpath(elem ...string) *S3Path {
	newKey := path.Join(append([]string{p.Key}, elem...)...)
	return &S3Path{
		Bucket: p.Bucket,
		Key:    newKey,
	}
}

func (p *S3Path) String() string {
	u := &url.URL{
		Scheme: scheme,
		Host:   p.Bucket,
		Path:   p.Key,
	}
	return u.String()
}

func Parse(s3URL string) (*S3Path, error) {
	if s3URL == "" {
		return nil, fmt.Errorf("empty URL")
	}
	u, err := url.Parse(s3URL)
	if err != nil {
		return nil, fmt.Errorf("could not parse URL %q: %v", s3URL, err)
	}

	if u.Scheme != scheme {
		return nil, fmt.Errorf("wrong s3 URL scheme: %q, expected: %q", u.Scheme, scheme)
	}

	return &S3Path{
		Bucket: u.Host,
		Key:    strings.TrimLeft(u.Path, "/"),
	}, nil
}
