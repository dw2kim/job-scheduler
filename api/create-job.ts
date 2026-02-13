import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { PutCommand } from '@aws-sdk/lib-dynamodb';
import { ddb, TABLE_EXECUTIONS } from '../shared/dynamo';

function toTimeBucket(iso: string) {
  const d = new Date(iso);
  d.setSeconds(0, 0);
  return d.toISOString().slice(0, 16); // yyyy-mm-ddTHH:MM
}

export const handler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {
  if (!event.body) {
    return { statusCode: 400, body: 'Missing body' };
  }

  const { runAt, task, params } = JSON.parse(event.body);

  if (!runAt || !task) {
    return { statusCode: 400, body: 'runAt and task required' };
  }

  const jobId = `job-${Date.now()}`;
  const timeBucket = toTimeBucket(runAt);
  const executionKey = `${runAt}.${jobId}`;

  await ddb.send(
    new PutCommand({
      TableName: TABLE_EXECUTIONS,
      Item: {
        timeBucket,
        executionKey,
        jobId,
        task,
        params,
        status: 'PENDING',
        attempt: 0,
      },
    })
  );

  return {
    statusCode: 201,
    body: JSON.stringify({
      jobId,
      status: 'PENDING',
    }),
  };
};