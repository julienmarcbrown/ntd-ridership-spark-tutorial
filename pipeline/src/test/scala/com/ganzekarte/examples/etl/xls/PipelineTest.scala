package com.ganzekarte.examples.etl.xls

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite


class PipelineTest extends AnyFunSuite with BeforeAndAfterAll {

  // Create a Spark session for testing
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .appName("PipelineTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("run method runs pipeline") {
    // Ensure no Spark session is active
    SparkSession.clearActiveSession()
    val resourceURL = getClass.getResource("test_data_june_2023_ridership.xlsx")

    Pipeline.run(resourceURL.getPath, Seq())

    assert(SparkSession.getActiveSession.isDefined, "Expected a Spark session to be initialized")
  }

}
