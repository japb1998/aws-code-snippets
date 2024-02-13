package ddb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jap1998/aws-code-snippets/aws/configuration"
)

var (
	ErrNotFound = errors.New("Item not found")
)

type DynamoDB struct {
	client *dynamodb.Client
}

// MustGetClient inits a new client with default options if option Fns are not provided otherwise it uses the defaults, if an error occurs it panics.
func MustGetClient(ctx context.Context, optFns ...func(*dynamodb.Options)) *DynamoDB {
	c := configuration.MustGetConfig(ctx)
	client := dynamodb.NewFromConfig(c, optFns...)

	return &DynamoDB{client: client}
}

// GetItem is a wrapper around dynamodb.GetItem with an already initialized client
func (d *DynamoDB) GetItem(ctx context.Context, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return d.client.GetItem(ctx, input)
}

// PutItem is a wrapper around dynamodb.PutItem with an already initialized client
func (d *DynamoDB) PutItem(ctx context.Context, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return d.client.PutItem(ctx, input)
}

// UpdateItem is a wrapper around dynamodb.UpdateItem with an already initialized client
func (d *DynamoDB) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	return d.client.UpdateItem(ctx, input)
}

// DeleteItem is a wrapper around dynamodb.DeleteItem with an already initialized client
func (d *DynamoDB) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return d.client.DeleteItem(ctx, input)
}

// Query is a wrapper around dynamodb.Query with an already initialized client
func (d *DynamoDB) Query(ctx context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	return d.client.Query(ctx, input)
}

// Scan is a wrapper around dynamodb.Scan with an already initialized client
func (d *DynamoDB) Scan(ctx context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return d.client.Scan(ctx, input)
}

// -- custom -- //
// UpdateIfExistsOrFail is a wrapper around dynamodb.UpdateItem with an already initialized client that updates an item if it exists or returns an error if it doesn't
func (d *DynamoDB) UpdateIfExistsOrFail(ctx context.Context, input *dynamodb.UpdateItemInput) error {
	// Check if item exists
	_, err := d.GetOne(ctx, &dynamodb.GetItemInput{
		TableName: input.TableName,
		Key:       input.Key,
	})

	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return ErrNotFound
		}
		return err
	}

	_, err = d.client.UpdateItem(ctx, input)

	if err != nil {
		return err
	}

	return nil
}

// GetOne is a wrapper around dynamodb.GetItem with an already initialized client that gets the first item that matches the provided key or error ErrNotFound if no item is found
func (d *DynamoDB) GetOne(ctx context.Context, input *dynamodb.GetItemInput) (item map[string]types.AttributeValue, err error) {
	output, err := d.client.GetItem(ctx, input)

	if err != nil {
		return nil, err
	}

	if output.Item == nil {
		return nil, ErrNotFound
	}

	return output.Item, nil
}

// ScanAll is a wrapper around dynamodb.Scan that takes keeps fetching dynamo until it retrieves all items with the provided query
func (d *DynamoDB) ScanAll(ctx context.Context, input *dynamodb.ScanInput) ([]map[string]types.AttributeValue, error) {
	items := make([]map[string]types.AttributeValue, 0)
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		input.ExclusiveStartKey = lastEvaluatedKey
		output, err := d.client.Scan(ctx, input)
		if err != nil {
			return nil, err
		}
		items = append(items, output.Items...)
		if output.LastEvaluatedKey == nil {
			break
		}
		lastEvaluatedKey = output.LastEvaluatedKey
	}

	return items, nil
}

// QueryWithPagination is a wrapper around dynamodb.Query that takes pagination options and returns a PaginatedResults struct
// pagination limits and queryInput.Limit are not the same, the former is the maximum number of items to return and the latter is the maximum number of items to return per page
func (d *DynamoDB) QueryWithPagination(ctx context.Context, input *PaginationOps) (*PaginatedResults, error) {
	var wg sync.WaitGroup
	var items = make([]map[string]types.AttributeValue, 0)
	var count int
	var errChan = make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()

		var lastEvaluatedKey map[string]types.AttributeValue

		for {
			input.ExclusiveStartKey = lastEvaluatedKey
			output, err := d.client.Query(ctx, &input.QueryInput)

			if err != nil {
				errChan <- fmt.Errorf("error querying dynamo: %w", err)
				return
			}

			var l int
			if len(output.Items)+len(items) > input.Limit {
				l = input.Limit - len(items)
			} else {
				l = len(output.Items)
			}

			items = append(items, output.Items[:l]...)

			if output.LastEvaluatedKey == nil || len(items) >= input.Limit {
				break
			}

			lastEvaluatedKey = output.LastEvaluatedKey
		}

		if input.Skip >= len(items) {
			items = slices.Delete(items, 0, len(items))
		} else {
			items = items[input.Skip:]
		}

		errChan <- nil
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := d.GetQueryCount(ctx, input.QueryInput)

		if err != nil {
			errChan <- fmt.Errorf("error getting query count: %w", err)
			return
		}

		count = c

		errChan <- nil
	}()

	go func() {
		wg.Wait()
		close(errChan)
	}()

	errs := make([]error, 0)
	for err := range errChan {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &PaginatedResults{
		Items: items,
		Skip:  input.Skip,
		Limit: input.Limit,
		Count: count,
	}, nil
}

// QueryAll is a wrapper around dynamodb.Query that takes keeps fetching dynamo until it retrieves all items with the provided query
func (d *DynamoDB) QueryAll(ctx context.Context, input *dynamodb.QueryInput) ([]map[string]types.AttributeValue, error) {

	items := make([]map[string]types.AttributeValue, 0)
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		input.ExclusiveStartKey = lastEvaluatedKey
		output, err := d.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		items = append(items, output.Items...)
		if output.LastEvaluatedKey == nil {
			break
		}
		lastEvaluatedKey = output.LastEvaluatedKey
	}

	return items, nil
}

// GetQueryCount is a wrapper around dynamodb.Query that returns the count of items that match the provided query. if input.Select is not types.SelectCount it will be set to types.SelectCount
func (d *DynamoDB) GetQueryCount(ctx context.Context, input dynamodb.QueryInput) (int, error) {

	if input.Select != types.SelectCount {
		input.Select = types.SelectCount
	}

	output, err := d.client.Query(ctx, &input)
	if err != nil {
		return 0, err
	}
	return int(output.Count), nil
}
