# aws-scripts

Collection of scripts to work with AWS from the CLI.

## sqs-dump.py

I use this script to download all messages that are available on a dead letter queue to my local machine.

It downloads the payloads into a `payloads` subfolder.

The payloads are usually in JSON format. Sometimes I want to transfer the JSON payloads to CSV and combine them into one big file.

### Converting multiple JSON files into one CSV file

Combine all JSON payloads into one big JSON array of payloads:

```
echo '[' > all.json.tmp
sed 's/^}/},/' *.json >> all.json.tmp
sed -i '' '$ s/.$//' all.json.tmp
echo ']' >> all.json.tmp
mv all.json.tmp all.json
```

I use the Visual Studion Code [JSON to CSV extension](https://marketplace.visualstudio.com/items?itemName=khaeransori.json2csv) to convert the combined JSON file to CSV (usage: `CMD SHIFT P / JSON to CSV`).
