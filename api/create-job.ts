import {
  APIGatewayProxyEvent,
  APIGatewayProxyResult,
} from 'aws-lambda';
import { PutCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { ddb, TABLE_EXECUTIONS } from '../shared/dynamo';

function toTimeBucket(iso: string) {
  const d = new Date(iso);
  d.setSeconds(0, 0);
  return d.toISOString().slice(0, 16); // YYYY-MM-DDTHH:mm
}

export const handler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {
  try {
    if (!event.body) {
      return { statusCode: 400, body: 'Missing body' };
    }

    const { runAt, task, params, idempotencyKey } =
      JSON.parse(event.body);

      // Parse runAt as Date, validate ISO format and future time
      const runAtDate = new Date(runAt);
      if (Number.isNaN(runAtDate.getTime())) {
        return { statusCode: 400, body: 'runAt must be a valid ISO date' };
      }
      if (runAtDate.getTime() <= Date.now()) {
        return { statusCode: 400, body: 'runAt must be in the future' };
      }

    // ✅ Required fields
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

    // 🔴 CRITICAL FIX #1 — validate timestamp
    const runAtMs = Date.parse(runAt);
    if (Number.isNaN(runAtMs)) {
      return {
        statusCode: 400,
        body: 'Invalid runAt timestamp',
      };
    }

    const nowMs = Date.now();

    // 🔴 CRITICAL FIX #2 — prevent past jobs
    if (runAtMs <= nowMs) {
      return {
        statusCode: 400,
        body: 'runAt must be in the future',
      };
    }

    // 🔴 CRITICAL FIX #3 — enforce UTC format
    // (prevents timezone bugs from FE)
    if (!runAt.endsWith('Z')) {
      return {
        statusCode: 400,
        body: 'runAt must be UTC ISO format (must end with Z)',
      };
    }

    // 1️⃣ Idempotency check
    const existing = await ddb.send(
      new QueryCommand({
        TableName: TABLE_EXECUTIONS,
        IndexName: 'gsi_idempotency',
        KeyConditionExpression: 'idempotencyKey = :key',
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
          createdAt: new Date().toISOString(), // ✅ nice for debugging
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
  } catch (err) {
    console.error('create-job error', err);
    return {
      statusCode: 500,
      body: 'Internal server error',
    };
  }
};