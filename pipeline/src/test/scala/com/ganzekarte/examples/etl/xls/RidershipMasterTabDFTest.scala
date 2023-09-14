package com.ganzekarte.examples.etl.xls

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class RidershipMasterTabDFTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  implicit var spark: SparkSession = _

  override def beforeEach(): Unit = {
    spark = SparkSession.builder
      .appName("RidershipMasterTabDFTest")
      .master("local[2]")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    spark.stop()
  }

  //  "buildDF" should "return a DataFrame with checksum column" in {
  //    // Mock the path to the Excel file or use a sample Excel file
  //    val path = "mock_or_sample_path_to_excel.xlsx"
  //    val df = RidershipMasterTabDF.buildDF(path).dataframe()
  //
  //    df.columns should contain ("checksum")
  //  }

  "xlsFromPath" should "read an Excel file and return RidershipMasterTabDF" in {
    val resourceURL = getClass.getResource("test_data_june_2023_ridership.xlsx")
    val targetPath = resourceURL.getPath
    val df = RidershipMasterTabDF.xlsFromPath(targetPath).dataframe()


    // Validate schema or content as per the sample file/mock data
  }

  "schemaFromDF" should "generate correct schema based on field definitions" in {
    val initialDF = spark.createDataFrame(Seq(
      ("A", 1.0, "2021_01")
    )).toDF("name", "value", "date")

    val schema = RidershipMasterTabDF.schemaFromDF(initialDF)
    schema.fieldNames should contain theSameElementsAs Seq("name", "value", "date")
  }

  "withChecksumColumn" should "add a checksum column to the DataFrame" in {
    val initialDF = spark.createDataFrame(Seq(
      ("A", "B", 1.0),
      ("X", "Y", 2.0)
    )).toDF("field1", "field2", "value")

    val ridershipDF = new RidershipMasterTabDF(initialDF)
    val updatedDF = ridershipDF.withChecksumColumn.dataframe()

    updatedDF.columns should contain("checksum")
  }

}
