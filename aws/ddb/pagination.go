package ddb

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type PaginationOps struct {
	dynamodb.QueryInput
	Skip  int
	Limit int
}

type PaginatedResults struct {
	Items []map[string]types.AttributeValue
	Skip  int
	Limit int
	Count int
}
