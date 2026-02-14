import {
  APIGatewayProxyEvent,
  APIGatewayProxyResult,
} from 'aws-lambda';
import { PutCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { ddb, TABLE_EXECUTIONS } from '../shared/dynamo';

function toTimeBucket(iso: string) {
  const d = new Date(iso);
  d.setSeconds(0, 0);
  return d.toISOString().slice(0, 16);
}

export const handler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {
  if (!event.body) {
    return { statusCode: 400, body: 'Missing body' };
  }

  const { runAt, task, params, idempotencyKey } =
    JSON.parse(event.body);

  if (!runAt || !task) {
    return {
      statusCode: 400,
      body: 'runAt and task required',
    };
  }

  if (!idempotencyKey) {
    return {
      statusCode: 400,
      body: 'idempotencyKey required',
    };
  }

  // 1️⃣ Check if job already exists
  const existing = await ddb.send(
    new QueryCommand({
      TableName: TABLE_EXECUTIONS,
      IndexName: 'gsi_idempotency',
      KeyConditionExpression:
        'idempotencyKey = :key',
      ExpressionAttributeValues: {
        ':key': idempotencyKey,
      },
    })
  );

  if (existing.Items && existing.Items.length > 0) {
    const job = existing.Items[0];
    return {
      statusCode: 200,
      body: JSON.stringify({
        jobId: job.jobId,
        status: job.status,
        idempotent: true,
      }),
    };
  }

  // 2️⃣ Create new job
  const jobId = `job-${Date.now()}`;
  const timeBucket = toTimeBucket(runAt);
  const executionKey = `${runAt}#${jobId}`;

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
        idempotencyKey,
      },
    })
  );

  return {
    statusCode: 200,
    body: JSON.stringify({
      jobId,
      status: 'PENDING',
      idempotent: false,
    }),
  };
};