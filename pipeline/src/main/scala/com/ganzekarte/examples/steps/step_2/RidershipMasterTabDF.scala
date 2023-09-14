package com.ganzekarte.examples.steps.step_2


import com.crealytics.spark.excel.ExcelDataFrameReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object RidershipMasterTabDF {


  def xlsFromPath(path: String)(implicit spark: SparkSession): DataFrame = {
    val df = spark.read.excel().load(path)
    df
  }


}