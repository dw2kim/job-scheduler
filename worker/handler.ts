import { SQSEvent } from 'aws-lambda';
import { UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { ddb, TABLE_EXECUTIONS } from '../shared/dynamo';

const MAX_ATTEMPTS = 3;

async function fakeExecuteJob(jobId: string) {
  await new Promise((res) => setTimeout(res, 1000));
  console.log(`Executed job ${jobId}`);
}

function ttlInDays(days: number): number {
  return Math.floor(Date.now() / 1000) + days * 24 * 60 * 60;
}

export const handler = async (event: SQSEvent) => {
  for (const record of event.Records) {
    const message = JSON.parse(record.body);
    const { timeBucket, executionKey, jobId } = message;

    const receiveCount = Number(
      record.attributes.ApproximateReceiveCount ?? 1
    );

    console.log('Worker received:', {
      jobId,
      executionKey,
      receiveCount,
    });

    try {
      // 🔴 Too many retries → permanent failure
      if (receiveCount > MAX_ATTEMPTS) {
        console.log('Max attempts exceeded, marking permanently failed');

        await ddb.send(
          new UpdateCommand({
            TableName: TABLE_EXECUTIONS,
            Key: { timeBucket, executionKey },
            UpdateExpression:
              'SET #status = :failed, #ttlEpochSeconds = :ttl',
            ExpressionAttributeNames: {
              '#status': 'status',
              '#ttlEpochSeconds': 'ttlEpochSeconds',
            },
            ExpressionAttributeValues: {
              ':failed': 'FAILED_PERMANENT',
              ':ttl': ttlInDays(14),
            },
          })
        );

        // Let SQS send to DLQ naturally
        return;
      }

      // 🟢 Execute job
      await fakeExecuteJob(jobId);

      // 🟢 Mark success
      await ddb.send(
        new UpdateCommand({
          TableName: TABLE_EXECUTIONS,
          Key: { timeBucket, executionKey },
          UpdateExpression:
            'SET #status = :succeeded, #ttlEpochSeconds = :ttl',
          ExpressionAttributeNames: {
            '#status': 'status',
            '#ttlEpochSeconds': 'ttlEpochSeconds',
          },
          ExpressionAttributeValues: {
            ':succeeded': 'SUCCEEDED',
            ':ttl': ttlInDays(7),
          },
        })
      );

      console.log('Marked SUCCEEDED:', executionKey);
    } catch (err) {
      console.error('Execution failed, will retry', err);

      // Keep status RUNNING for retry visibility
      await ddb.send(
        new UpdateCommand({
          TableName: TABLE_EXECUTIONS,
          Key: { timeBucket, executionKey },
          UpdateExpression: 'SET #status = :running',
          ExpressionAttributeNames: {
            '#status': 'status',
          },
          ExpressionAttributeValues: {
            ':running': 'RUNNING',
          },
        })
      );

      // ❗ IMPORTANT: rethrow so SQS retries
      throw err;
    }
  }
};