#
# Download all available messages from an SQS queue. Especially useful for dead letter queues.
# Only works if the queue has a limited number of messages and a high enough visibility timeout.
# Authentication against the queue works with the 'default' profile in ~/.aws/credentials.
# 
# Usage:
#   
#   python3 sqs-dump.py QUEUE_URL
# 
# AWS will place a message back on the queue if it hasn't been deleted within the visibility timeout.
# To avoid infinite loops & repeated receives of the same message this script caps the max number of reads.
# 
# For queues with a default visibility timeout (30s) and limited number of available messages (<200?) 
# this script will, under normal circumstances, receive each message once and then stop polling.
# 
import boto3
import json
import os
import sys
import time
from datetime import datetime

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


def to_datetime_string(ts_in_ms):
  ts_in_seconds=ts_in_ms/1000
  return datetime.fromtimestamp(ts_in_seconds).strftime(DATETIME_STRING_FORMAT)


def determine_base_filename(message):
  message_id=message["MessageId"]
  sent_timestamp=message["Attributes"]["SentTimestamp"]
  sent_datetime=to_datetime_string(int(sent_timestamp))
  return sent_datetime + "_" + message_id


def save_message(message, folder):
  body=message["Body"]
  payload=json.dumps(json.loads(json.loads(body)["Message"]), indent=2)

  filename=determine_base_filename(message)
  filename_message=os.path.join(os.getcwd(), folder, "messages", filename) + ".message.json"
  filename_payload=os.path.join(os.getcwd(), folder, "payloads", filename) + ".payload.json"

  write_to_file(filename_message, body)
  write_to_file(filename_payload, payload)


def queue_name(queue_url):
  return queue_url.rsplit('/', 1)[1]


def create_target_folder(queue_url):
  q_name=queue_name(queue_url).replace("dead-letter-queue","dlq")
  target_folder=datetime.now().strftime(DATETIME_STRING_FORMAT) + "_" + q_name
  os.mkdir(target_folder)
  os.mkdir(os.path.join(target_folder, "messages"))
  os.mkdir(os.path.join(target_folder, "payloads"))
  return target_folder


def increment(i):
    sys.stdout.write('.')
    sys.stdout.flush()
    return i+1


def download_queue(queue_url, target_folder):
  i=0
  message=receive_message(queue_url)
  while (i < MAX_RECEIVES) and (message != None):
    save_message(message, target_folder)
    i=increment(i)
    message=receive_message(queue_url)
  return i

def main():
  if len(sys.argv) != 2:
    raise Exception("Missing argument (the queue URL)")
  queue_url=sys.argv[1]

  print("")
  print(queue_name(queue_url))
  start=time.time()
  target_folder=create_target_folder(queue_url)
  nr_messages=download_queue(queue_url, target_folder)
  print("\nDownloaded {} messages in {} seconds".format(nr_messages, round(time.time()-start,1)))


if  __name__ == "__main__":
  main()  
