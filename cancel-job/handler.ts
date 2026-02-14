import { APIGatewayProxyHandlerV2 } from 'aws-lambda';
import {
  ScanCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { ddb, TABLE_EXECUTIONS } from '../shared/dynamo';

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  const jobId = event.pathParameters?.jobId;

  if (!jobId) {
    return {
      statusCode: 400,
      body: JSON.stringify({ message: 'jobId required' }),
    };
  }

  try {
    // 🔎 Step 1: find execution by jobId (temporary scan)
    const scan = await ddb.send(
      new ScanCommand({
        TableName: TABLE_EXECUTIONS,
        FilterExpression: 'jobId = :jobId',
        ExpressionAttributeValues: {
          ':jobId': jobId,
        },
        Limit: 1,
      })
    );

    const item = scan.Items?.[0];

    if (!item) {
      return {
        statusCode: 404,
        body: JSON.stringify({ message: 'Job not found' }),
      };
    }

    // 🛑 Step 2: only cancel if still PENDING
    await ddb.send(
      new UpdateCommand({
        TableName: TABLE_EXECUTIONS,
        Key: {
          timeBucket: item.timeBucket,
          executionKey: item.executionKey,
        },
        UpdateExpression: 'SET #status = :cancelled',
        ConditionExpression: '#status = :pending',
        ExpressionAttributeNames: {
          '#status': 'status',
        },
        ExpressionAttributeValues: {
          ':cancelled': 'CANCELLED',
          ':pending': 'PENDING',
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