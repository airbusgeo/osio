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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

type httpmock struct {
	*http.Client
	ok *bool
}

func (m httpmock) Do(req *http.Request) (*http.Response, error) {
	fmt.Println(req)
	if req.Method == "HEAD" {
		*m.ok = true
		return &http.Response{
			StatusCode: http.StatusMethodNotAllowed,
			Body:       ioutil.NopCloser(&bytes.Buffer{}),
		}, nil
	}
	return m.Client.Do(req)
}

func TestHTTP(t *testing.T) {
	ctx := context.Background()
	hh, _ := HTTPHandle(ctx)
	httpa, _ := NewAdapter(hh)

	// bucket not found
	_, err := httpa.Reader("https://storage.googleapis.com/godal-ci-data-public-notexists/test.tif")
	assert.Equal(t, err, syscall.ENOENT)

	// object not found
	_, err = httpa.Reader("https://storage.googleapis.com/godal-ci-data-public/test-notexists.tif")
	assert.Equal(t, err, syscall.ENOENT)

	// check total size
	r, err := httpa.Reader("https://storage.googleapis.com/godal-ci-data-public/test.tif")
	assert.NoError(t, err)
	assert.Equal(t, int64(212), r.Size())

	// check fallback
	var ok bool
	hh, _ = HTTPHandle(ctx, HTTPClient(httpmock{Client: &http.Client{}, ok: &ok}))
	httpa, _ = NewAdapter(hh)
	r, err = httpa.Reader("https://storage.googleapis.com/godal-ci-data-public/test.tif")
	assert.NoError(t, err)
	assert.Equal(t, int64(212), r.Size())
	assert.True(t, ok)
}
