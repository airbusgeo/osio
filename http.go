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
	"fmt"
	"io"
	"net/http"
	"syscall"
)

type Client interface {
	Do(*http.Request) (*http.Response, error)
}

type HTTPHandler struct {
	ctx                context.Context
	client             Client
	requestMiddlewares []func(*http.Request)
}

// HTTPOption is an option that can be passed to RegisterHandler
type HTTPOption func(o *HTTPHandler)

// HTTPClient sets the http.Client that will be used by the handler
func HTTPClient(cl Client) HTTPOption {
	return func(o *HTTPHandler) {
		o.client = cl
	}
}

// HTTPBasicAuth sets user/pwd for each request
func HTTPBasicAuth(username, password string) HTTPOption {
	return func(o *HTTPHandler) {
		o.requestMiddlewares = append(o.requestMiddlewares, func(req *http.Request) {
			req.SetBasicAuth(username, password)
		})
	}
}

// HTTPHeader sets a header on http request. Useful to add api keys.
func HTTPHeader(key, value string) HTTPOption {
	return func(o *HTTPHandler) {
		o.requestMiddlewares = append(o.requestMiddlewares, func(req *http.Request) {
			req.Header.Add(key, value)
		})
	}
}

// HTTPHandle creates a KeyReaderAt suitable for constructing an Adapter
// that accesses objects using the http protocol
func HTTPHandle(ctx context.Context, opts ...HTTPOption) (*HTTPHandler, error) {
	handler := &HTTPHandler{
		ctx: ctx,
	}
	for _, o := range opts {
		o(handler)
	}
	if handler.client == nil {
		handler.client = &http.Client{}
	}
	return handler, nil
}

func handleResponse(r *http.Response) (int64, error) {
	if r.StatusCode == 404 {
		return -1, syscall.ENOENT
	}
	if r.StatusCode == 416 {
		return 0, io.EOF
	}
	return 0, fmt.Errorf("reader for %s: status code %d", r.Request.URL.String(), r.StatusCode)
}

func (h *HTTPHandler) keySize(key string) (int64, error) {
	req, _ := http.NewRequestWithContext(h.ctx, "HEAD", key, nil)
	for _, mw := range h.requestMiddlewares {
		mw(req)
	}
	r, err := h.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("reader for %s: %w", key, err)
	}
	defer r.Body.Close()
	if r.StatusCode == 200 {
		return r.ContentLength, nil
	}
	if r.StatusCode != 403 && r.StatusCode != 405 {
		return handleResponse(r)
	}
	// retry with get
	req.Method = "GET"
	r, err = h.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("reader for %s: %w", key, err)
	}
	defer r.Body.Close()
	if r.StatusCode == 200 {
		return r.ContentLength, nil
	}
	return handleResponse(r)
}

func (h *HTTPHandler) StreamAt(key string, off int64, n int64) (io.ReadCloser, int64, error) {
	// HEAD request to get object size as it is not returned in range requests
	var size int64
	var err error
	if off == 0 {
		size, err = h.keySize(key)
		if err != nil {
			return nil, size, err
		}
	}
	// GET request to fetch range
	req, _ := http.NewRequestWithContext(h.ctx, "GET", key, nil)
	for _, mw := range h.requestMiddlewares {
		mw(req)
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", off, off+n-1))
	r, err := h.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("reader for %s: %w", key, err)
	}
	if r.StatusCode != 200 && r.StatusCode != 206 {
		size, err = handleResponse(r)
		return nil, size, err
	}
	return r.Body, size, err
}

func (h *HTTPHandler) ReadAt(key string, p []byte, off int64) (int, int64, error) {
	panic("deprecated (kept for retrocompatibility)")
}
