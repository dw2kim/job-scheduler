import { QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { ddb, TABLE_EXECUTIONS } from '../shared/dynamo';

const sqs = new SQSClient({ region: 'us-east-2' });

const QUEUE_URL = process.env.QUEUE_URL!;

function getTimeBuckets(now: Date, minutesAhead: number): string[] {
  const buckets: string[] = [];

  for (let i = 0; i <= minutesAhead; i++) {
    const d = new Date(now.getTime() + i * 60_000);
    d.setSeconds(0, 0);
    buckets.push(d.toISOString().slice(0, 16)); // YYYY-MM-DDTHH:MM
  }

  return buckets;
}

export const handler = async () => {
  const now = new Date();
  const buckets = getTimeBuckets(now, 5);
  // const buckets = ['2026-02-07T17:05'];

  console.log('Watcher running for buckets:', buckets);

  for (const bucket of buckets) {
    const result = await ddb.send(
      new QueryCommand({
        TableName: TABLE_EXECUTIONS,
        KeyConditionExpression: 'timeBucket = :tb',
        FilterExpression: '#status = :pending',
        ExpressionAttributeNames: {
          '#status': 'status',
        },
        ExpressionAttributeValues: {
          ':tb': bucket,
          ':pending': 'PENDING',
        },
      })
    );

    const items = result.Items ?? [];

    for (const item of items) {
      // Send to SQS
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: QUEUE_URL,
          MessageBody: JSON.stringify({
            jobId: item.jobId,
            executionKey: item.executionKey,
            timeBucket: item.timeBucket,
            attempt: item.attempt,
          }),
        })
      );

      // Mark as RUNNING (conditionally)
      await ddb.send(
        new UpdateCommand({
          TableName: TABLE_EXECUTIONS,
          Key: {
            timeBucket: item.timeBucket,
            executionKey: item.executionKey,
          },
          UpdateExpression: 'SET #status = :running',
          ConditionExpression: '#status = :pending',
          ExpressionAttributeNames: {
            '#status': 'status',
          },
          ExpressionAttributeValues: {
            ':running': 'RUNNING',
            ':pending': 'PENDING',
          },
        })
      );

      console.log('Enqueued execution:', item.executionKey);
    }
  }
};

// 👇 ONLY for local execution
if (require.main === module) {
  handler().catch(console.error);
}