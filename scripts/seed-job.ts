/*
This script will:
	•	Compute a run time
	•	Compute timeBucket
	•	Compute executionKey
	•	Insert a real execution row
*/

import { PutCommand } from '@aws-sdk/lib-dynamodb';
import { ddb, TABLE_EXECUTIONS } from '../shared/dynamo';

function toTimeBucket(date: Date): string {
  // Round down to minute
  date.setSeconds(0, 0);
  return date.toISOString().slice(0, 16); // YYYY-MM-DDTHH:MM
}

async function seed() {
  const jobId = `job-${Date.now()}`;

  const executionTime = new Date(Date.now() + 60_000); // now + 1 min
  const timeBucket = toTimeBucket(new Date(executionTime));
  const executionKey = `${executionTime.toISOString()}#${jobId}`;

  const item = {
    timeBucket,
    executionKey,
    jobId,
    status: 'PENDING',
    attempt: 0,
  };

  await ddb.send(
    new PutCommand({
      TableName: TABLE_EXECUTIONS,
      Item: item,
    })
  );

  console.log('Seeded execution:', item);
}

seed().catch(console.error);