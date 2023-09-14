package com.ganzekarte.examples.etl.xls


import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TimeSeriesDataTabDFTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  implicit var spark: SparkSession = _

  override def beforeEach(): Unit = {
    spark = SparkSession.builder
      .appName("TimeSeriesDataTabDFTest")
      .master("local[2]")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    spark.stop()
  }

  "TimeSeriesDataTabDF" should "correctly add a checksum column" in {
    val initialDF = spark.createDataFrame(Seq(
      ("A", "2021_01", 10.0),
      ("B", "2021_02", 20.0)
    )).toDF("name", "date", "value")

    val tsDF = new TimeSeriesDataTabDF(initialDF).withChecksumColumn
    tsDF.dataframe().columns should contain("checksum")
  }

  it should "drop non-temporal columns correctly" in {
    val initialDF = spark.createDataFrame(Seq(
      ("A", "2021_01", 10.0, "nonTemporal"),
      ("B", "2021_02", 20.0, "nonTemporal")
    )).toDF("name", "date", "value", "Status")

    val tsDF = new TimeSeriesDataTabDF(initialDF).withOnlyTemporalColumns
    tsDF.dataframe().columns should not contain "Status"
  }

  it should "melt date columns into rows" in {
    val initialDF = spark.createDataFrame(Seq(
      (10.0, 20.0, "abadfgasgda")
    )).toDF("2021_01", "2021_02", "checksum")

    val tsDF = new TimeSeriesDataTabDF(initialDF).withColumnsMelted("value")
    val meltedDF = tsDF.dataframe()

    meltedDF.select("date").distinct().count() shouldBe 2
  }

}

