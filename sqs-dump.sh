#!/bin/bash
aws_url="https://sqs.eu-west-3.amazonaws.com/666"
dump_json="~/aws-scripts/sqs-dump.py"
dump_csv="~/aws-scripts/sqs-dump-to-csv.py"

declare -a dlqs=(
  "queue1"
  "queue2"
  "queue3"
)

printf "DOWNLOADING TO CSV\n"
for dlq in "${dlqs[@]}"
do 
  python3 $dump_csv "${aws_url}/${dlq}"
done

printf "\nWAITING FOR MESSAGES TO RE-APPEAR ON THE DLQs ...\n\n"
sleep 35 # assuming a message visibility timeout of 30 sec on each queue

printf "DOWNLOADING TO JSON\n"
for dlq in "${dlqs[@]}"
do 
  python3 $dump_json "${aws_url}/${dlq}"
done
