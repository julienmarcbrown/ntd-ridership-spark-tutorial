package com.ganzekarte.examples.etl.steps.step_2

import com.crealytics.spark.excel.ExcelDataFrameReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object RidershipMasterTabDF {


  def xlsFromPath(path: String)(implicit spark: SparkSession): DataFrame = {
    val df = spark.read.excel().load(path)
    df
  }


}