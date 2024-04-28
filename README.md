## Description
This is a log scraping project written in scala. main purpose behing this project is when code written in spark, we can log dataset to log file. This scala script will help to scrape the log file and load dataset to the spark-shell so these datasets can be queried to debug.

## Usage
Download the spark from [here](https://spark.apache.org/downloads.html)

Navigate to spark-hadoop folder.

```
./spark-shell -i <script file path>

```
In this case we can use the src/main/scala/LogScraper.scala. 

Example
```
./spark-shell -i /path/to/spark-log-scraper-scala/src/main/scala/LogScraper.scala
```

After that it will create an object inside spark shell. We can call main method as below

```agsl
LogScraper.main(Array("/Users/kd/git/spark/spark-log-scraper-scala/src/main/resources/log.txt"))
```
