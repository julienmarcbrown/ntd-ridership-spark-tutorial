package com.ganzekarte.examples.etl.xls


import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class JoinedTabsDFTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  implicit var spark: SparkSession = _

  override def beforeEach(): Unit = {
    spark = SparkSession.builder
      .appName("JoinedTabsDFTest")
      .master("local[2]")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    spark.stop()
  }

  "fromMasterTimeSeriesDFPair" should "join two dataframes based on checksum and sanitize columns" in {
    // Sample DFs (modify as needed)
    val masterTabDF = spark.createDataFrame(Seq(
      ("checksum1", "los angeles"),
      ("checksum2", "new york mta")
    )).toDF("checksum", "agency")

    val timeSeriesDataTabDF = spark.createDataFrame(Seq(
      ("checksum1", "11_23"),
      ("checksum2", "12_23")
    )).toDF("checksum", "date")

    val joinedDF = JoinedTabsDF.fromMasterTimeSeriesDFPair(new RidershipMasterTabDF(masterTabDF), new TimeSeriesDataTabDF(timeSeriesDataTabDF)).dataframe()

    joinedDF.columns should contain theSameElementsAs Seq("checksum", "date", "agency")
    joinedDF.count() shouldEqual 2
  }

  "sanitizeColumnNames" should "replace spaces with underscores and convert to lowercase" in {
    val originalName = "Sample Column Name"
    val sanitized = JoinedTabsDF.sanitizeColumnNames(originalName)
    sanitized shouldEqual "sample_column_name"
  }

  "withSanitizedColumns" should "sanitize all column names in the dataframe" in {
    val initialDF = spark.createDataFrame(Seq(
      ("A", "B")
    )).toDF("First Column", "Second Column")

    val joinedDF = new JoinedTabsDF(initialDF).withSanitizedColumns.dataframe()

    joinedDF.columns should contain theSameElementsAs Seq("first_column", "second_column")
  }
}
