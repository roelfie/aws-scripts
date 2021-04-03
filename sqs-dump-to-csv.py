#
# Download all available messages from an SQS queue and write them to one single CSV file.
# This script assumes that all message payloads are in JSON format. Otherwise it will fail.
# Only top-level JSON fields are included in the output CSV. Nested fields aren't.
# Output makes most sense if all payloads are structurally identical (have the same fields).
# Less suitable for heterogeneous message queues.
#
# Usage:
#   
#   python3 sqs-dump-to-csv.py QUEUE_URL
# 
# See sqs-dump.py for more information.
#
import boto3
import csv
import json
import os
import sys
import time
from dateutil import parser
from datetime import datetime
from functools import reduce

sqs = boto3.client('sqs')

MAX_RECEIVES=100
DATETIME_STRING_FORMAT="%Y%m%d_%H%M%S"


def receive_message(queue_url):
  response=sqs.receive_message(
    QueueUrl=queue_url,
    AttributeNames=['All'],
    MessageAttributeNames=['All'],
    MaxNumberOfMessages=1,
    VisibilityTimeout=30,
    WaitTimeSeconds=1
  )
  try:
    return response["Messages"][0]
  except:
    return None


def write_to_file(filename, data):
  file = open(filename, "w")
  file.write(data)
  file.close()


def timestamp_to_string(ts_in_ms):
  ts_in_seconds=ts_in_ms/1000
  return datetime.fromtimestamp(ts_in_seconds).strftime(DATETIME_STRING_FORMAT)


def datetime_zulu_to_string(datetime_zulu_string):
  dt = parser.isoparse(datetime_zulu_string)
  return dt.strftime(DATETIME_STRING_FORMAT)


# Return the payload as a dict, enriched with sqs message data
def to_dict(message):
  body=json.loads(message["Body"])
  message_id=body["MessageId"]
  timestamp=datetime_zulu_to_string(body["Timestamp"])
  topic=body["TopicArn"].rsplit(':', 1)[1]

  payload=json.loads(body["Message"])
  payload["_Sqs_MessageId"]=message_id
  payload["_Sqs_Timestamp"]=timestamp
  payload["_Sns_Topic"]=topic
  return payload


def queue_name(queue_url):
  return queue_url.rsplit('/', 1)[1]


def create_csv_filename(queue_url):
  q_name=queue_name(queue_url).replace("dead-letter-queue","dlq")
  filename=datetime.now().strftime(DATETIME_STRING_FORMAT) + "_" + q_name + ".csv"
  return filename


def increment(i):
    sys.stdout.write('.')
    sys.stdout.flush()
    return i+1


def download_payloads(queue_url):
  i=0
  payloads=[]
  message=receive_message(queue_url)
  while (i < MAX_RECEIVES) and (message != None):
    payload=to_dict(message)
    payloads.append(payload)
    i=increment(i)
    message=receive_message(queue_url)
  return payloads


def unique_keys(dict_list):
  keys_list=list(map(lambda d: set(d.keys()), dict_list))
  keys=reduce((lambda set1, set2: set1.union(set2)), keys_list)
  return keys


def to_csv_file(payloads, filename):
  headers=sorted(unique_keys(payloads))
  with open(filename, 'w', newline='') as csvfile: 
    writer=csv.DictWriter(csvfile, fieldnames=headers)
    writer.writeheader()
    for p in payloads:
      writer.writerow(p)


def download_queue(queue_url, filename):
  payloads=download_payloads(queue_url)
  to_csv_file(payloads, filename)
  return len(payloads)


def main():
  if len(sys.argv) != 2:
    raise Exception("Missing argument (the queue URL)")
  queue_url=sys.argv[1]

  print("")
  print(queue_name(queue_url))
  start=time.time()
  filename=create_csv_filename(queue_url)
  nr_messages=download_queue(queue_url, filename)
  print("\nDownloaded {} messages in {} seconds".format(nr_messages, round(time.time()-start,1)))


if  __name__ == "__main__":
  main()  
