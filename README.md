# MapReduce4SentimentAnalysis
Reference/Credit:
- for word counter source code:
-- Prof. Donald J. Patterson(https://youtu.be/ce857_wtR-U)
-- Vijay Khanna(https://github.com/vijay-khanna/aws-emr-demos)
- for sentiment analysis(https://docs.cloudera.com/documentation/other/tutorial/CDH5/topics/ht_example_4_sentiment_analysis.html)

How to build:
mvn package

How to run:

- CLI 
java -cp ./target/MapReduceSample.jar mypackage.SentimentWordCount ./input/twitter.csv output ./wordlist/pos-words.txt ./wordlist/neg-words.txt

- MS vscode configuration
{
    "type": "java",
    "name": "sentiment-twitter",
    "request": "launch",
    "mainClass": "myproject.SentimentWordCount",
    "args": "./input/twitter.csv output ./wordlist/pos-words.txt ./wordlist/neg-words.txt"
}