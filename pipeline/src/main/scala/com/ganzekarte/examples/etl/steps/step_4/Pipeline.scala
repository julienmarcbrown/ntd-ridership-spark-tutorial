package com.ganzekarte.examples.etl.steps.step_4

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Pipeline {

  def main(args: Array[String]): Unit = {
    run(args(0), args.tail)
  }
  def run(path: String, inputTabs: Seq[String]): Unit = {
    // Create a Spark configuration
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "4g")

    // Create a Spark session
    implicit val spark: SparkSession =
      SparkSession.builder
        .config(conf = conf)
        .appName("entrypoint")
        .getOrCreate()

    // Define the target tabs for processing
    val targetTabs = if (inputTabs.nonEmpty) inputTabs else Seq("UPT", "VRM", "VRH", "VOMS")


    val ridershipMasterDF = RidershipMasterTabDF.xlsFromPath(path = path)


    val timeSeriesTabDF = TimeSeriesDataTabDF.buildDF(path = path, targetTabs = targetTabs)

  }

}