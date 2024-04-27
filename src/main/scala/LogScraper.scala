import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.spark.sql.types._



object LogScraper {

  def main(args: Array[String]) = {

    print("hello world")

    val spark = SparkSession.builder()
      .appName("Log Scraper")
      .master("local[*]")
      .getOrCreate()

    val titlePattern = """XYZ_DATASET: \{(.*?)\}""".r
    val entryPattern = """XYZ_DATA_ENTRY: \{(.*?)\}""".r
    val extractPattern: Regex = """.*:\s*\{([^}]*)\}""".r;
    var lastDataset = ""

    val groupedDataset = new mutable.HashMap[String, List[String]];

    val lines: Dataset[String] = spark.read.textFile("./log.txt")

    val filteredDataset: Dataset[String] = lines
      .filter(col("value")
        .startsWith("|")
        .or(col("value")
          .contains("XYZ_DATASET")))

    val filtered = filteredDataset.coalesce(1)
      .collectAsList()
      .stream()
      .map { e =>
        val doesPatternExisit = titlePattern.findFirstIn(e).isDefined
        val formattedString = ""
        if (doesPatternExisit) {
          lastDataset = extractPattern.findFirstMatchIn(e) match {
            case Some(matched) => matched.group(1)
            case None => ""
          }
          titlePattern.findFirstIn(e).get
        } else {
          val row = e.replaceFirst("\\|", "")
          formattedString
            .concat("XYZ_DATA_ENTRY: {")
            .concat(row)
            .concat(lastDataset)
            .concat("}")

        }
      }.toList

    filtered.forEach { e =>
      val isTitleRow = titlePattern.findFirstIn(e).isDefined
      if (isTitleRow) {
        titlePattern.findFirstMatchIn(e) match {
          case Some(matchedValue) => {

            val title = matchedValue.group(1);
            println("title:" + title)
            if (!groupedDataset.contains(title)) {
              groupedDataset.put(title, List.empty[String])
            }
          }
        }
      }
      else {
        entryPattern.findFirstMatchIn(e) match {
          case Some(matchedValue) => {
            val dataRow = matchedValue.group(1)
            val datasetKey = matchedValue.group(1).split("\\|").last;
            groupedDataset.get(datasetKey) match {
              case Some(xs: List[String]) => groupedDataset(datasetKey) = xs :+ dataRow
            }
          }
        }
      }
    }

    groupedDataset.foreach { case (k, v) =>

      val columnNames = v.head.split("\\|")
      val rows = v.tail.map(_.split("\\|")).map(fields => Row.fromSeq(fields))
      val schema = StructType(columnNames.map(name => StructField(name, StringType)))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      println("CREATING TABLE FOR DATASET: "+k)
      df.createOrReplaceTempView(k)
    }

    print("****FINISHED******")

  }
}
