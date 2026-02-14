import { APIGatewayProxyHandlerV2 } from 'aws-lambda';
import {
  QueryCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { ddb, TABLE_EXECUTIONS } from '../shared/dynamo';

function ttlInDays(days: number): number {
  return Math.floor(Date.now() / 1000) + days * 24 * 60 * 60;
}

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  const jobId = event.pathParameters?.jobId;

  if (!jobId) {
    return {
      statusCode: 400,
      body: JSON.stringify({ message: 'jobId required' }),
    };
  }

  try {
    // 🔎 Step 1: Query using GSI (FAST)
    const query = await ddb.send(
      new QueryCommand({
        TableName: TABLE_EXECUTIONS,
        IndexName: 'gsi_jobId', // 👈 important
        KeyConditionExpression: 'jobId = :jobId',
        ExpressionAttributeValues: {
          ':jobId': jobId,
        },
        Limit: 1,
      })
    );

    const item = query.Items?.[0];

    if (!item) {
      return {
        statusCode: 404,
        body: JSON.stringify({ message: 'Job not found' }),
      };
    }

    // 🛑 Step 2: Only cancel if still PENDING
    await ddb.send(
      new UpdateCommand({
        TableName: TABLE_EXECUTIONS,
        Key: {
          timeBucket: item.timeBucket,
          executionKey: item.executionKey,
        },
        UpdateExpression:
          'SET #status = :cancelled, #ttlEpochSeconds = :ttl',
        ConditionExpression: '#status = :pending',
        ExpressionAttributeNames: {
          '#status': 'status',
          '#ttlEpochSeconds': 'ttlEpochSeconds',
        },
        ExpressionAttributeValues: {
          ':cancelled': 'CANCELLED',
          ':pending': 'PENDING',
          ':ttl': ttlInDays(7),
        },
      })
    );

    return {
      statusCode: 200,
      body: JSON.stringify({
        jobId,
        status: 'CANCELLED',
      }),
    };
  } catch (err: any) {
    if (err.name === 'ConditionalCheckFailedException') {
      return {
        statusCode: 409,
        body: JSON.stringify({
          message: 'Job already running or finished',
        }),
      };
    }

    console.error('Cancel failed', err);

    return {
      statusCode: 500,
      body: JSON.stringify({ message: 'Internal error' }),
    };
  }
};