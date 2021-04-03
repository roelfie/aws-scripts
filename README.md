# aws-scripts

Collection of scripts to work with AWS from the CLI.

## Tips & Tricks

### Manually converting multiple JSON files into one CSV file

```
echo '[' > all.json.tmp
sed 's/^}/},/' *.json >> all.json.tmp
sed -i '' '$ s/.$//' all.json.tmp
echo ']' >> all.json.tmp
mv all.json.tmp all.json
```

Use [JSON to CSV extension](https://marketplace.visualstudio.com/items?itemName=khaeransori.json2csv) for Visual Studio Code to convert the combined JSON file to CSV (on a Mac: Cmd-Shift-P / JSON to CSV).
