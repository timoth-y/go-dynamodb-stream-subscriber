# go-dynamodb-stream-subscriber
Go channel for streaming Dynamodb Updates

```go
package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/timoth-y/go-dynamodb-stream-subscriber/stream"
)

func main() {
	cfg := aws.NewConfig().WithRegion("eu-west-1")
	streamSvc := dynamodbstreams.New(cfg)
	dynamoSvc := dynamodb.New(cfg)
	table := "tableName"

	streamSubscriber := stream.NewStreamSubscriber(dynamoSvc, streamSvc, table)
	ch, errCh := streamSubscriber.GetStreamDataAsync()

	go func(errCh <-chan error) {
		for err := range errCh {
			fmt.Println("Stream Subscriber error: ", err)
		}
	}(errCh)

	for record := range ch {
		fmt.Println("from channel:", record)
	}
}
```
