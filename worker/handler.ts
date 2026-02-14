import { SQSEvent } from 'aws-lambda';
import { UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { ddb, TABLE_EXECUTIONS } from '../shared/dynamo';

const MAX_ATTEMPTS = 3;


function ttlInDays(days: number): number {
  return Math.floor(Date.now() / 1000) + days * 24 * 60 * 60;
}

async function sendTelegramMessage(text: string) {
  const token = process.env.TELEGRAM_BOT_TOKEN!;
  const chatId = process.env.TELEGRAM_CHAT_ID!;

  const url = `https://api.telegram.org/bot${token}/sendMessage`;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      chat_id: chatId,
      text,
    }),
  });

  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`Telegram API failed: ${errText}`);
  }
}

async function executeRealJob(jobId: string) {
  await sendTelegramMessage(
    `✅ Job executed: ${jobId}\nTime: ${new Date().toISOString()}`
  );
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
      await executeRealJob(jobId);

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