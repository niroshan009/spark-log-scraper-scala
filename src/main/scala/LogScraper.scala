import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.util.matching.Regex
import org.apache.spark.sql.types._

import scala.io.StdIn



object LogScraper {

//  def main() = {
  def main(args: Array[String]) = {

    val logFilePath =  args(0) //args[0].toString();

    if(logFilePath == null) {
      throw new Exception("Invalid log file path")
    }

    val spark = SparkSession.builder()
      .appName("Log Scraper")
      .master("local[*]")
      .getOrCreate()

    val titlePattern = """XYZ_DATASET: \{(.*?)\}""".r
    val entryPattern = """XYZ_DATA_ENTRY: \{(.*?)\}""".r
    val extractPattern: Regex = """.*:\s*\{([^}]*)\}""".r;
    var lastDataset = ""

    val groupedDataset = new mutable.HashMap[String, List[String]];

    println("Loading file from path: "+ logFilePath)

    val lines: Dataset[String] = spark.read.textFile(logFilePath)

    val filteredDataset: Dataset[String] = lines
      .filter(col("value")
        .startsWith("|")
        .or(col("value")
          .contains("XYZ_DATASET")))

    val filtered : List[String] = filteredDataset.rdd.coalesce(1)
      .map { e =>
        val doesPatternExisit = titlePattern.findFirstIn(e).isDefined
        val formattedString = ""
        if (doesPatternExisit) {
          lastDataset = extractPattern.findFirstMatchIn(e) match {
            case Some(matched) => matched.group(1).asInstanceOf[String]
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
      }.collect().toList

    filtered.foreach { e =>
      val isTitleRow = titlePattern.findFirstIn(e).isDefined
      if (isTitleRow) {
        titlePattern.findFirstMatchIn(e) match {
          case Some(matchedValue) => {

            val title = matchedValue.group(1).asInstanceOf[String];
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
            val dataRow = matchedValue.group(1).asInstanceOf[String]
            val datasetKey = matchedValue.group(1).asInstanceOf[String].split("\\|").last;
            groupedDataset.get(datasetKey) match {
              case Some(xs: List[String]) => groupedDataset(datasetKey) = xs :+ dataRow
              case None => print("non")
            }
          }
        }
      }
    }

    println("****STARTING TO CREATE DATASETS****")
    groupedDataset.foreach { case (k, v) =>
      val columnNames : List[String]= v.head.split("\\|").map(_.trim).toList
      val rows = v.tail.map(_.split("\\|")).map(fields => Row.fromSeq(fields))
      val schema = StructType(columnNames.map(name => StructField(name, StringType)))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      println("CREATING TABLE FOR DATASET: "+k)
      df.createOrReplaceTempView(k)
    }

    println("****FINISHED CREATING DATASETS******")

  }
}
