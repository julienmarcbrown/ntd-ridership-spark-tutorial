package com.ganzekarte.examples.etl.steps.step_4

import com.crealytics.spark.excel.ExcelDataFrameReader
import com.ganzekarte.examples.etl.steps.step_4.FieldTransformationDefinitions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object TimeSeriesDataTabDF {

  /**
   * Processes time series data from given Excel tabs and merges them into a single dataframe.
   *
   * @param path       The path to the Excel file.
   * @param targetTabs The list of tab names to process.
   * @param spark      Implicit Spark session instance.
   * @return A dataframe with processed time series data.
   */
  def buildDF(path: String, targetTabs: Seq[String])(implicit spark: SparkSession) = {
    // Process each target tab and return a sequence of dataframes.
    val dfTabs = targetTabs.map { tabName =>
      val dfForTab = xlsFromPath(path, tabName)

    }
  }

  def xlsFromPath(path: String, tabName: String)(implicit spark: SparkSession): DataFrame = {
    val peekDF = spark.read
      .option("locale", "en-US")
      .excel(
        header = true,
        dataAddress = tabName + "!",
        inferSchema = false,
        usePlainNumberFormat = false,
        maxRowsInMemory = 100000000,
        maxByteArraySize = 100000000,
      ).load(path)

    val schema = schemaFromDF(peekDF)

    val df = spark.read
      .option("locale", "en-US")
      .excel(
        header = true,
        dataAddress = tabName + "!",
        inferSchema = false,
        usePlainNumberFormat = false,
        maxRowsInMemory = 100000000,
        maxByteArraySize = 100000000,
      ).schema(schema).load(path)

    df
  }

  def schemaFromDF(df: DataFrame): StructType = {
    val fields = df.columns.map { header =>
      FieldDefinitions.find(_.excelTabName == header) match {
        case Some(field) =>
          val dataType = field.dataType match {
            case "FloatType" => FloatType
            case "IntegerType" | "LongType" => LongType
            case _ => StringType
          }
          StructField(header, dataType, nullable = true)
        case None =>
          StructField(header, StringType, nullable = true)
      }
    }
    StructType(fields)
  }
}