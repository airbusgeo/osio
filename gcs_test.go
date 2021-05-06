// Copyright 2021 Airbus Defence and Space
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
	"syscall"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
)

func TestGCS(t *testing.T) {
	ctx := context.Background()
	stcl, _ := storage.NewClient(ctx, option.WithoutAuthentication())
	gcs, _ := GCSHandle(ctx, GCSClient(stcl))
	gcsa, _ := NewAdapter(gcs)
	_, err := gcsa.Reader("gs://godal-ci-data-public/gdd/doesnotexist.tif")
	assert.Equal(t, err, syscall.ENOENT)
	r, err := gcsa.Reader("gs://godal-ci-data-public/test.tif")
	assert.NoError(t, err)
	assert.Equal(t, int64(212), r.Size())
	r, err = gcsa.Reader("godal-ci-data-public/test.tif")
	assert.NoError(t, err)
	assert.Equal(t, int64(212), r.Size())

	//invalid bucket/object
	_, err = gcsa.Reader("gs://godal-ci-data-public")
	assert.Error(t, err)
	//invalid bucket/object
	_, err = gcsa.Reader("godal-ci-data-public/")
	assert.Error(t, err)

	//unauthenticated access
	_, err = gcsa.Reader("godal-ci-data/test-notexists.tif")
	assert.Error(t, err)
}
