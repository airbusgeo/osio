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

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
)

func TestS3(t *testing.T) {
	ctx := context.Background()
	s3cl := s3.New(s3.Options{
		Region:      "us-west-2",
		Credentials: nil,
	})
	sss, _ := S3Handle(ctx, S3Client(s3cl))
	s3a, _ := NewAdapter(sss)

	// bucket not found
	_, err := s3a.Reader("s3://ukn-bucket/gdd/doesnotexist.tif")
	assert.Equal(t, err, syscall.ENOENT)

	// object not found
	_, err = s3a.Reader("s3://sentinel-cogs/test.tif")
	assert.Equal(t, err, syscall.ENOENT)

	// check total size
	r, err := s3a.Reader("s3://sentinel-cogs/sentinel-s2-l2a-cogs/60/V/XL/2019/5/S2A_60VXL_20190521_1_L2A/TCI.tif")
	assert.NoError(t, err)
	assert.Equal(t, int64(1252564), r.Size())
	r, err = s3a.Reader("sentinel-cogs/sentinel-s2-l2a-cogs/60/V/XL/2019/5/S2A_60VXL_20190521_1_L2A/TCI.tif")
	assert.NoError(t, err)
	assert.Equal(t, int64(1252564), r.Size())

	//invalid bucket/object uri
	_, err = s3a.Reader("s3://sentinel-cogs")
	assert.Error(t, err)

	//invalid bucket/object uri
	_, err = s3a.Reader("sentinel-cogs/")
	assert.Error(t, err)

	//unauthenticated access
	_, err = s3a.Reader("sentinel-cogs/test-notexists.tif")
	assert.Error(t, err)
}
