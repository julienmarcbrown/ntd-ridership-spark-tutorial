package com.ganzekarte.examples.steps.step_1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Pipeline {

  def main(args: Array[String]): Unit = {
    ???
  }
  def run(): Unit = {
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

  }

}