package com.ganzekarte.examples.steps.step_5

import com.crealytics.spark.excel.ExcelDataFrameReader
import com.ganzekarte.examples.steps.step_5.FieldTransformationDefinitions._
import org.apache.spark.sql.functions.{col, concat_ws, md5}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object RidershipMasterTabDF {
  type DF = DataFrame

  /**
   * Builds the RidershipMasterTabDF from an Excel file located at the provided path.
   * It also adds a checksum column to the resulting data frame.
   *
   * @param path    The path to the Excel file.
   * @param headers A sequence of XLS column headers to be used when constructing the DataFrame schema.
   * @param spark   Implicit SparkSession instance for data processing.
   * @return An instance of RidershipMasterTabDF with the added checksum column.
   */
  def buildDF(path: String)(implicit spark: SparkSession): RidershipMasterTabDF = {
    // Create RidershipMasterTabDF from the given path
    val ridershipMasterTabDF = xlsFromPath(path)
    // Add the checksum column to the dataframe and return the updated RidershipMasterTabDF
    ridershipMasterTabDF
      .withChecksumColumn
  }

  def xlsFromPath(path: String)(implicit spark: SparkSession): RidershipMasterTabDF = {
    val peekDF = spark.read.excel(
      header = true,
      inferSchema = true,
      dataAddress = "Master!",
    ).load(path)

    val schema = schemaFromDF(peekDF)

    val df = spark.read.excel(
      header = true,
      inferSchema = true,
      dataAddress = "Master!",
    ).schema(schema).load(path)

    new RidershipMasterTabDF(df)
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

class RidershipMasterTabDF(df: RidershipMasterTabDF.DF) {
  /**
   * Adds a checksum column to the dataframe for later cross-referencing.
   *
   * @return Instance of RidershipMasterDataTabDF with the added checksum column.
   */
  def withChecksumColumn: RidershipMasterTabDF = {
    val filteredColumns = df.columns.filter(ChecksumFields.map(_.excelTabName).contains(_)).sorted
    val concatenatedColumns = concat_ws("", filteredColumns.map(col): _*)
    val checksum = md5(concatenatedColumns)
    new RidershipMasterTabDF(df.withColumn("checksum", checksum))
  }

  /**
   * Accessor method for the internal dataframe.
   *
   * @return Internal dataframe.
   */
  def dataframe(): RidershipMasterTabDF.DF = {
    df
  }
}