import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

export const TABLE_EXECUTIONS = 'executions';

const client = new DynamoDBClient({
  region: 'us-east-2',
});

export const ddb = DynamoDBDocumentClient.from(client);