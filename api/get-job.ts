import {
    APIGatewayProxyEvent,
    APIGatewayProxyResult,
  } from 'aws-lambda';
  import { QueryCommand } from '@aws-sdk/lib-dynamodb';
  import { ddb, TABLE_EXECUTIONS } from '../shared/dynamo';
  
  export const handler = async (
    event: APIGatewayProxyEvent
  ): Promise<APIGatewayProxyResult> => {
    const jobId = event.pathParameters?.jobId;
  
    if (!jobId) {
      return { statusCode: 400, body: 'Missing jobId' };
    }
  
    const res = await ddb.send(
      new QueryCommand({
        TableName: TABLE_EXECUTIONS,
        IndexName: 'gsi_jobId',
        KeyConditionExpression: 'jobId = :jobId',
        ExpressionAttributeValues: {
          ':jobId': jobId,
        },
        ScanIndexForward: true, // oldest → newest
      })
    );
  
    const executions = res.Items ?? [];
  
    if (executions.length === 0) {
      return { statusCode: 404, body: 'Job not found' };
    }
  
    // Derive overall status
    const statuses = executions.map((e) => e.status);
  
    let overallStatus = 'PENDING';
    if (statuses.includes('RUNNING')) overallStatus = 'RUNNING';
    else if (statuses.every((s) => s === 'SUCCEEDED'))
      overallStatus = 'SUCCEEDED';
    else if (statuses.includes('FAILED_PERMANENT'))
      overallStatus = 'FAILED';
  
    return {
      statusCode: 200,
      body: JSON.stringify({
        jobId,
        status: overallStatus,
        executions,
      }),
    };
  };