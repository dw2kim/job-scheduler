import { SQSEvent } from 'aws-lambda';
import { UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { ddb, TABLE_EXECUTIONS } from '../shared/dynamo';

const MAX_ATTEMPTS = 3;

async function fakeExecuteJob(jobId: string) {
  await new Promise((res) => setTimeout(res, 1000));
  console.log(`Executed job ${jobId}`);
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
      if (receiveCount > MAX_ATTEMPTS) {
        console.log('Max attempts exceeded, marking permanently failed');

        await ddb.send(
          new UpdateCommand({
            TableName: TABLE_EXECUTIONS,
            Key: { timeBucket, executionKey },
            UpdateExpression: 'SET #status = :failed',
            ExpressionAttributeNames: { '#status': 'status' },
            ExpressionAttributeValues: {
              ':failed': 'FAILED_PERMANENT',
            },
          })
        );

        // Let SQS move it to DLQ
        return;
      }

      await fakeExecuteJob(jobId);

      await ddb.send(
        new UpdateCommand({
          TableName: TABLE_EXECUTIONS,
          Key: { timeBucket, executionKey },
          UpdateExpression: 'SET #status = :succeeded',
          ExpressionAttributeNames: { '#status': 'status' },
          ExpressionAttributeValues: {
            ':succeeded': 'SUCCEEDED',
          },
        })
      );

      console.log('Marked SUCCEEDED:', executionKey);
    } catch (err) {
      console.error('Execution failed, will retry', err);

      await ddb.send(
        new UpdateCommand({
          TableName: TABLE_EXECUTIONS,
          Key: { timeBucket, executionKey },
          UpdateExpression: 'SET #status = :running',
          ExpressionAttributeNames: { '#status': 'status' },
          ExpressionAttributeValues: {
            ':running': 'RUNNING',
          },
        })
      );

      throw err; // important: let SQS retry
    }
  }
};