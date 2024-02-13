package configuration

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

// MustGetConfig returns a new aws.Config with default options
func MustGetConfig(ctx context.Context) aws.Config {
	/*
		here is where you would add custom profile, region, etc. if defaults are not setup.
		example:
		c, err = config.LoadDefaultConfig(ctx, func(lo *config.LoadOptions) error {
		lo.Region = "us-west-2"
		return nil
		})

		example 2:
		config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile("personal"), config.WithRegion("us-east-1"))
	*/
	c, err := config.LoadDefaultConfig(ctx)

	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	return c
}
