package osio_test

import (
	"archive/zip"
	"context"
	"fmt"

	"github.com/airbusgeo/osio"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func WithS3Region(region string) func(opts *s3.Options) {
	return func(opts *s3.Options) {
		opts.Region = region
	}
}

func ExampleS3Handle() {
	ctx := context.Background()

	cfg, _ := config.LoadDefaultConfig(ctx)
	s3cl := s3.NewFromConfig(cfg, WithS3Region("eu-central-1"))
	s3r, _ := osio.S3Handle(ctx, osio.S3Client(s3cl), osio.S3RequestPayer())
	osr, _ := osio.NewAdapter(s3r)

	uri := "s3://sentinel-s2-l1c-zips/S2A_MSIL1C_20210630T074611_N0300_R135_T48XWN_20210630T082841.zip"
	obj, _ := osr.Reader(uri)
	zipf, _ := zip.NewReader(obj, obj.Size())

	for _, f := range zipf.File {
		fmt.Printf("%s\n", f.Name)
	}
}
