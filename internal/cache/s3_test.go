package cache_test

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/cache/cachetest"
	"github.com/block/sfptc/internal/logging"
)

// TestS3Cache tests the S3 cache implementation.
//
// This test requires the following environment variables to be set:
// - S3_TEST_ENDPOINT: S3 endpoint (e.g., localhost:9000 for local minio, or s3.amazonaws.com for AWS)
// - S3_TEST_BUCKET: S3 bucket name
// - S3_TEST_REGION: S3 region (optional, defaults to us-east-1)
// - S3_TEST_USE_SSL: Whether to use SSL (optional, defaults to true)
//
// For credentials, you have two options:
//
// Option 1: Explicit credentials (required for local minio):
// - S3_TEST_ACCESS_KEY_ID: S3 access key ID
// - S3_TEST_SECRET_ACCESS_KEY: S3 secret access key
//
// Option 2: AWS credential chain (for AWS S3 or EC2 instances with IAM roles):
// - Leave S3_TEST_ACCESS_KEY_ID and S3_TEST_SECRET_ACCESS_KEY unset
// - The test will use the standard AWS credential chain:
//  1. AWS environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
//  2. AWS credentials file (~/.aws/credentials)
//  3. IAM role from EC2 instance metadata
//
// To run tests against a local minio server:
//
//	docker run -d -p 9000:9000 -p 9001:9001 \
//	  -e "MINIO_ROOT_USER=minioadmin" \
//	  -e "MINIO_ROOT_PASSWORD=minioadmin" \
//	  minio/minio server /data --console-address ":9001"
//
//	# Create a test bucket using mc (minio client)
//	docker exec <container-id> mc alias set local http://localhost:9000 minioadmin minioadmin
//	docker exec <container-id> mc mb local/test-bucket
//
//	export S3_TEST_ENDPOINT=localhost:9000
//	export S3_TEST_ACCESS_KEY_ID=minioadmin
//	export S3_TEST_SECRET_ACCESS_KEY=minioadmin
//	export S3_TEST_BUCKET=test-bucket
//	export S3_TEST_USE_SSL=false
//	go test -v ./internal/cache -run TestS3Cache
//
// To run tests against AWS S3 with IAM credentials:
//
//	export S3_TEST_ENDPOINT=s3.amazonaws.com
//	export S3_TEST_BUCKET=my-test-bucket
//	export S3_TEST_REGION=us-east-1
//	# AWS credentials will be picked up from environment, credentials file, or IAM role
//	go test -v ./internal/cache -run TestS3Cache
func TestS3Cache(t *testing.T) {
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	bucket := os.Getenv("S3_TEST_BUCKET")

	if endpoint == "" || bucket == "" {
		t.Skip("Skipping S3 cache tests: S3_TEST_ENDPOINT and S3_TEST_BUCKET environment variables must be set")
	}

	// Credentials are optional - will use AWS credential chain if not provided
	accessKeyID := os.Getenv("S3_TEST_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("S3_TEST_SECRET_ACCESS_KEY")

	region := os.Getenv("S3_TEST_REGION")
	if region == "" {
		region = "us-west-2"
	}

	useSSL := true
	if os.Getenv("S3_TEST_USE_SSL") == "false" {
		useSSL = false
	}

	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
		c, err := cache.NewS3(ctx, cache.S3Config{
			Endpoint:        endpoint,
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			Bucket:          bucket,
			Region:          region,
			UseSSL:          useSSL,
			MaxTTL:          100 * time.Millisecond,
		})
		assert.NoError(t, err)
		return c
	})
}
