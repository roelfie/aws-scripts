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
import os
import sys
import boto3
import json
from datetime import datetime

sqs = boto3.client('sqs')

MAX_RECEIVES=60
QUEUE_URL=''


def receive_message():
  response = sqs.receive_message(
    QueueUrl=QUEUE_URL,
    AttributeNames=['All'],
    MessageAttributeNames=['All'],
    MaxNumberOfMessages=1,
    VisibilityTimeout=30,
    WaitTimeSeconds=1
  )
  try:
    messages = response["Messages"]
    if len(messages) == 0:
      return None
    return messages[0]
  except:
    return None


def write_to_file(filename, data):
  file = open(filename, "w")
  file.write(data)
  file.close()


def save_message(message, folder):
  message_id = message["MessageId"]
  sent_timestamp = message["Attributes"]["SentTimestamp"]
  sent_datetime = datetime.fromtimestamp(int(sent_timestamp)/1000).strftime("%Y%m%d_%H%M%S")
  body = message["Body"]

  cwd = os.getcwd()
  base_filename = sent_datetime + "_" + message_id

  write_to_file(os.path.join(cwd, folder, base_filename) + ".json", 
                body
  )

  body_message = json.dumps(json.loads(json.loads(body)["Message"]), indent=2)
  write_to_file(os.path.join(cwd, folder, "msg", base_filename) + ".msg.json", 
                body_message
  )


def main():
  if len(sys.argv) != 2:
    raise Exception("Missing argument (the queue URL)")

  global QUEUE_URL
  QUEUE_URL = sys.argv[1]
  print(QUEUE_URL)
  queue_name=QUEUE_URL.rsplit('/', 1)[1]

  target_folder = datetime.now().strftime("%Y%m%d_%H%M%S_") + queue_name
  os.mkdir(target_folder)
  os.mkdir(os.path.join(target_folder, "msg"))

  i = 0
  while True:
    i = i+1
    print("Receiving message ", i)
    message = receive_message()
    if (i > MAX_RECEIVES) or (message == None):
      break
    save_message(message, target_folder)


if  __name__ == "__main__":
  main()  
