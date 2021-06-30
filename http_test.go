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

	"github.com/stretchr/testify/assert"
)

func TestHTTP(t *testing.T) {
	ctx := context.Background()
	hh, _ := HTTPHandle(ctx)
	httpa, _ := NewAdapter(hh)

	// bucket not found
	_, err := httpa.Reader("https://storage.googleapis.com/godal-ci-data-public/doesnotexist.tif")
	assert.Equal(t, err, syscall.ENOENT)

	// object not found
	_, err = httpa.Reader("https://storage.googleapis.com/godal-ci-data-public/test-notexists.tif")
	assert.Equal(t, err, syscall.ENOENT)

	// check total size
	r, err := httpa.Reader("https://storage.googleapis.com/godal-ci-data-public/test.tif")
	assert.NoError(t, err)
	assert.Equal(t, int64(212), r.Size())
	r, err = httpa.Reader("https://storage.googleapis.com/godal-ci-data-public/test.tif")
	assert.NoError(t, err)
	assert.Equal(t, int64(212), r.Size())
}
