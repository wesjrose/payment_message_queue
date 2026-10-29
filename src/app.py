# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os, boto3

sqs = boto3.client("sqs")

QUEUE_URL = os.getenv("QUEUE_URL")
BASE_DELAY_SECONDS = int(os.getenv("BASE_DELAY_SECONDS", "30"))
MAX_DELAY_SECONDS = int(os.getenv("MAX_DELAY_SECONDS", "900"))


def process_message(record):
    """
    This is the function that will handle message routing
    """
    # rasing an exceptions to test exponential backoff
    raise Exception("This is a test exception")

    print(f"Processed message: {record['body']}")


def backoff_delay(receive_count):
    return min(BASE_DELAY_SECONDS * 2 ** (receive_count - 1), MAX_DELAY_SECONDS)


def lambda_handler(event, context):
    if event:
        batch_item_failures = []
        sqs_batch_response = {}

        for record in event["Records"]:
            try:
                process_message(record)
            except Exception as e:
                receive_count = int(record["attributes"]["ApproximateReceiveCount"])
                delay = backoff_delay(receive_count)
                print(
                    f"Failed message {record['messageId']} (attempt {receive_count}): {e}. Retrying in {delay}s"
                )
                try:
                    sqs.change_message_visibility(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=record["receiptHandle"],
                        VisibilityTimeout=delay,
                    )
                except Exception as visibility_error:
                    print(f"could not set visibility timeout: {visibility_error}")

                batch_item_failures.append({"itemIdentifier": record["messageId"]})

        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response
