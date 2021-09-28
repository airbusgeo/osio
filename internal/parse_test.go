package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBucketObjectParsing(t *testing.T) {
	b, o, err := BucketObject("s3://bucket/object")
	assert.NoError(t, err)
	assert.Equal(t, "bucket", b)
	assert.Equal(t, "object", o)

	b, o, err = BucketObject("s3://bucket/subdir/object")
	assert.NoError(t, err)
	assert.Equal(t, "bucket", b)
	assert.Equal(t, "subdir/object", o)

	b, o, err = BucketObject("s3://bucket/subdir/object/")
	assert.NoError(t, err)
	assert.Equal(t, "bucket", b)
	assert.Equal(t, "subdir/object/", o)

	b, o, err = BucketObject("s3:///bucket/subdir/object/")
	assert.NoError(t, err)
	assert.Equal(t, "bucket", b)
	assert.Equal(t, "subdir/object/", o)

	b, o, err = BucketObject("/bucket/subdir/object/")
	assert.NoError(t, err)
	assert.Equal(t, "bucket", b)
	assert.Equal(t, "subdir/object/", o)

	b, o, err = BucketObject("///bucket/subdir/object/")
	assert.NoError(t, err)
	assert.Equal(t, "bucket", b)
	assert.Equal(t, "subdir/object/", o)

	b, o, err = BucketObject("/s3:/bucket/subdir/object/")
	assert.NoError(t, err)
	assert.Equal(t, "s3:", b)
	assert.Equal(t, "bucket/subdir/object/", o)

	b, o, err = BucketObject("/s3://bucket/subdir/object/")
	assert.NoError(t, err)
	assert.Equal(t, "s3:", b)
	assert.Equal(t, "/bucket/subdir/object/", o)

	b, o, err = BucketObject("/s3:///bucket/subdir/object/")
	assert.NoError(t, err)
	assert.Equal(t, "s3:", b)
	assert.Equal(t, "//bucket/subdir/object/", o)

	_, _, err = BucketObject("s3://bucket")
	assert.Error(t, err)
	_, _, err = BucketObject("s3://bucket/")
	assert.Error(t, err)
	_, _, err = BucketObject("s3:///bucket")
	assert.Error(t, err)
}
