package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jap1998/aws-code-snippets/aws/ddb"
)

var dy *ddb.DynamoDB

func main() {
	ctx := context.Background()
	dy := ddb.MustGetClient(ctx)

	// // scan all items
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	input := &dynamodb.ScanInput{
		TableName: aws.String(os.Getenv("TABLE_NAME")),
	}
	results, err := dy.ScanAll(ctxWithTimeout, input)

	for _, item := range results {
		fmt.Printf("Item: %+v\n", item)
	}

	// query items with pagination
	ops := &ddb.PaginationOps{
		Skip:  0,
		Limit: 3,
		QueryInput: dynamodb.QueryInput{
			TableName:              aws.String(os.Getenv("TABLE_NAME")),
			KeyConditionExpression: aws.String("#pk = :pk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "primaryKey",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "<your-primary-key>"},
			},
		},
	}

	pResults, err := dy.QueryWithPagination(ctx, ops)

	if err != nil {
		panic(err)
	}
	fmt.Println("Paginated Results", pResults.Count)
	for _, item := range pResults.Items {
		fmt.Printf("Paginated: %+v\n", item)
	}

	// this should error
	key, err := attributevalue.MarshalMap(map[string]string{
		"primaryKey": "no-exist",
		"sortKey":    "no-exist",
	})

	if err != nil {
		fmt.Println("Error marshalling key", err)
	}

	_, err = dy.GetOne(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(os.Getenv("TABLE_NAME")),
		Key:       key})

	if err != nil {
		fmt.Println("Error", err)
	}
}
