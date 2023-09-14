package com.ganzekarte.examples.steps.step_3

import com.crealytics.spark.excel.ExcelDataFrameReader
import com.ganzekarte.examples.steps.step_3.FieldTransformationDefinitions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object RidershipMasterTabDF {


  def xlsFromPath(path: String)(implicit spark: SparkSession): DataFrame = {
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