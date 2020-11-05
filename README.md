# MapReduce4SentimentAnalysis
How to build:
mvn package

How to run:

- CLI 
java -cp ./target/MapReduceSample.jar mypackage.SentimentWordCount ./input/input.txt output

- MS vscode configuration
{
    "type": "java",
    "name": "sentiment-twitter",
    "request": "launch",
    "mainClass": "myproject.SentimentWordCount",
    "args": "./input/twitter.csv output ./wordlist/pos-words.txt ./wordlist/neg-words.txt"
}


Reference:
- https://github.com/vijay-khanna/aws-emr-demos
- https://docs.cloudera.com