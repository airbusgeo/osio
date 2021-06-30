// Copyright 2021 Kayrros
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package osio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type S3Handler struct {
	ctx          context.Context
	client       *s3.Client
	requestPayer string
}

// S3Option is an option that can be passed to RegisterHandler
type S3Option func(o *S3Handler)

// S3Client sets the s3.Client that will be used by the handler
func S3Client(cl *s3.Client) S3Option {
	return func(o *S3Handler) {
		o.client = cl
	}
}

// S3Handle creates a KeyReaderAt suitable for constructing an Adapter
// that accesses objects on Amazon S3
func S3Handle(ctx context.Context, opts ...S3Option) (*S3Handler, error) {
	handler := &S3Handler{
		ctx: ctx,
	}
	for _, o := range opts {
		o(handler)
	}
	if handler.client == nil {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 client: %w", err)
		}
		handler.client = s3.NewFromConfig(cfg)
	}
	return handler, nil
}

func (h *S3Handler) ReadAt(key string, p []byte, off int64) (int, int64, error) {
	bucket, object := osuriparse("s3", key)
	if len(bucket) == 0 || len(object) == 0 {
		return 0, 0, fmt.Errorf("invalid key")
	}

	rng := fmt.Sprintf("Range: bytes=%d-%d", off, off+int64(len(p))-1)
	r, err := h.client.GetObject(h.ctx, &s3.GetObjectInput{
		Bucket:       &bucket,
		Key:          &object,
		RequestPayer: types.RequestPayer(h.requestPayer),
		Range:        &rng,
	})

	if err != nil {
		var ae smithy.APIError
		if off > 0 && errors.As(err, &ae) && ae.ErrorCode() == "InvalidRange" {
			return 0, 0, io.EOF
		}
		if errors.As(err, &ae) && (ae.ErrorCode() == "NoSuchBucket" || ae.ErrorCode() == "NoSuchKey") {
			return 0, -1, syscall.ENOENT
		}
		return 0, 0, fmt.Errorf("new reader for s3://%s/%s: %w", bucket, object, err)
	}
	defer r.Body.Close()
	n, err := io.ReadFull(r.Body, p)
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	return n, r.ContentLength, err
}
