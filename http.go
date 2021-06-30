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

type RequestMiddleware func(*http.Request)

type HTTPHandler struct {
	ctx                context.Context
	client             *http.Client
	requestMiddlewares []RequestMiddleware
}

// HTTPOption is an option that can be passed to RegisterHandler
type HTTPOption func(o *HTTPHandler)

// HTTPClient sets the http.Client that will be used by the handler
func HTTPClient(cl *http.Client) HTTPOption {
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

func (h *HTTPHandler) ReadAt(url string, p []byte, off int64) (int, int64, error) {
	req, _ := http.NewRequest("GET", url, nil)
	req = req.WithContext(h.ctx)
	for _, mw := range h.requestMiddlewares {
		mw(req)
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1))

	r, err := h.client.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("new reader for %s: %w", url, err)
	}

	if r.StatusCode == 404 {
		return 0, -1, syscall.ENOENT
	}
	if r.StatusCode == 416 {
		return 0, 0, io.EOF
	}
	if r.StatusCode != 200 && r.StatusCode != 206 {
		return 0, 0, fmt.Errorf("new reader for %s: status code %d", url, r.StatusCode)
	}
	defer r.Body.Close()
	n, err := io.ReadFull(r.Body, p)
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	return n, r.ContentLength, err
}
