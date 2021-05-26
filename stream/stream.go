package stream

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
)

type Subscriber struct {
	dynamoSvc         *dynamodb.Client
	streamSvc         *dynamodbstreams.Client
	table             *string
	ShardIteratorType dynamodbstreams.ShardIteratorType
	Limit             *int64
}

func NewStreamSubscriber(
	dynamoSvc *dynamodb.Client,
	streamSvc *dynamodbstreams.Client,
	table string,
) *Subscriber {
	return &Subscriber{
		dynamoSvc: dynamoSvc,
		streamSvc: streamSvc,
		table: &table,
		ShardIteratorType: dynamodbstreams.ShardIteratorTypeLatest,
	}
}

func (r *Subscriber) GetStreamData() (<-chan dynamodbstreams.Record, <-chan error) {

	ch := make(chan dynamodbstreams.Record, 1)
	errCh := make(chan error, 1)

	go func(ch chan<- dynamodbstreams.Record, errCh chan<- error) {
		var shardId *string
		var prevShardId *string
		var streamArn *string
		var err error

		for {
			prevShardId = shardId
			shardId, streamArn, err = r.findProperShardId(prevShardId)
			if err != nil {
				errCh <- err
			}
			if shardId != nil {
				err = r.processShardBackport(shardId, streamArn, ch)
				if err != nil {
					errCh <- err
					// reset shard id to process it again
					shardId = prevShardId
				}
			}
			if shardId == nil {
				time.Sleep(time.Second * 10)
			}

		}
	}(ch, errCh)

	return ch, errCh
}

func (r *Subscriber) GetStreamDataAsync() (<-chan dynamodbstreams.Record, <-chan error) {
	ch := make(chan dynamodbstreams.Record, 1)
	errCh := make(chan error, 1)

	needUpdateChannel := make(chan struct{}, 1)
	needUpdateChannel <- struct{}{}

	allShards := make(map[string]struct{})
	shardProcessingLimit := 5
	shardsCh := make(chan *dynamodbstreams.GetShardIteratorInput, shardProcessingLimit)
	lock := sync.Mutex{}

	go func() {
		tick := time.NewTicker(time.Minute)
		for {
			select {
			case <-tick.C:
				needUpdateChannel <- struct{}{}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-needUpdateChannel:
				streamArn, err := r.getLatestStreamArn()
				if err != nil {
					errCh <- err
					return
				}
				ids, err := r.getShardIds(streamArn)
				if err != nil {
					errCh <- err
					return
				}
				for _, sObj := range ids {
					lock.Lock()
					if _, ok := allShards[*sObj.ShardId]; !ok {
						allShards[*sObj.ShardId] = struct{}{}
						shardsCh <- &dynamodbstreams.GetShardIteratorInput{
							StreamArn:         streamArn,
							ShardId:           sObj.ShardId,
							ShardIteratorType: r.ShardIteratorType,
						}
					}
					lock.Unlock()
				}

			}
		}

	}()

	limit := make(chan struct{}, shardProcessingLimit)

	go func() {
		time.Sleep(time.Second * 10)
		for shardInput := range shardsCh {
			limit <- struct{}{}
			go func(sInput *dynamodbstreams.GetShardIteratorInput) {
				err := r.processShard(sInput, ch)
				if err != nil {
					errCh <- err
				}

				<-limit
			}(shardInput)
		}
	}()
	return ch, errCh
}

func (r *Subscriber) getShardIds(streamArn *string) (ids []dynamodbstreams.Shard, err error) {
	des, err := r.streamSvc.DescribeStreamRequest(&dynamodbstreams.DescribeStreamInput{
		StreamArn: streamArn,
	}).Send(context.Background())
	if err != nil {
		return nil, err
	}
	// No shards
	if 0 == len(des.StreamDescription.Shards) {
		return nil, nil
	}

	return des.StreamDescription.Shards, nil
}

func (r *Subscriber) findProperShardId(previousShardId *string) (shadrId *string, streamArn *string, err error) {
	streamArn, err = r.getLatestStreamArn()
	if err != nil {
		return nil, nil, err
	}
	des, err := r.streamSvc.DescribeStreamRequest(&dynamodbstreams.DescribeStreamInput{
		StreamArn: streamArn,
	}).Send(context.Background())
	if err != nil {
		return nil, nil, err
	}

	if 0 == len(des.StreamDescription.Shards) {
		return nil, nil, nil
	}

	if previousShardId == nil {
		shadrId = des.StreamDescription.Shards[0].ShardId
		return
	}

	for _, shard := range des.StreamDescription.Shards {
		shadrId = shard.ShardId
		if shard.ParentShardId != nil && *shard.ParentShardId == *previousShardId {
			return
		}
	}

	return
}

func (r *Subscriber) getLatestStreamArn() (*string, error) {
	tableInfo, err := r.dynamoSvc.DescribeTableRequest(
		&dynamodb.DescribeTableInput{TableName: r.table},
	).Send(context.Background())

	if err != nil {
		return nil, err
	}

	if nil == tableInfo.Table.LatestStreamArn {
		return nil, errors.New("empty table stream arn")
	}

	return tableInfo.Table.LatestStreamArn, nil
}

func (r *Subscriber) processShardBackport(shardId, lastStreamArn *string, ch chan<- dynamodbstreams.Record) error {
	return r.processShard(&dynamodbstreams.GetShardIteratorInput{
		StreamArn:         lastStreamArn,
		ShardId:           shardId,
		ShardIteratorType: r.ShardIteratorType,
	}, ch)
}

func (r *Subscriber) processShard(input *dynamodbstreams.GetShardIteratorInput, ch chan<- dynamodbstreams.Record) error {
	iter, err := r.streamSvc.GetShardIteratorRequest(input).Send(context.Background())
	if err != nil {
		return err
	}

	if iter.ShardIterator == nil {
		return nil
	}

	nextIterator := iter.ShardIterator

	for nextIterator != nil {
		recs, err := r.streamSvc.GetRecordsRequest(&dynamodbstreams.GetRecordsInput{
			ShardIterator: nextIterator,
			Limit:         r.Limit,
		}).Send(context.Background())
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "TrimmedDataAccessException" {
			return nil
		}
		if err != nil {
			return err
		}

		for _, record := range recs.Records {
			ch <- record
		}

		nextIterator = recs.NextShardIterator

		sleepDuration := time.Second

		// Nil next itarator, shard is closed
		if nextIterator == nil {
			sleepDuration = time.Millisecond * 10
		} else if len(recs.Records) == 0 {
			// Empty set, but shard is not closed -> sleep a little
			sleepDuration = time.Second * 10
		}

		time.Sleep(sleepDuration)
	}
	return nil
}
