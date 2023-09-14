package com.ganzekarte.examples.etl.steps.step_6

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The object `JoinedTabsDF` provides utility functions to join the dataframes
 * from `RidershipMasterTabDF` and `TimeSeriesDataTabDF`.
 */
object JoinedTabsDF {

  /**
   * Joins the dataframes from RidershipMasterTabDF and TimeSeriesDataTabDF based on the 'checksum' column.
   * Filters out rows where columns UPT, VRM, VRH, and VOMS are all zero.
   *
   * @param masterTabDF         Instance of RidershipMasterTabDF containing the master data.
   * @param timeSeriesDataTabDF Instance of TimeSeriesDataTabDF containing time series data.
   * @return A new instance of JoinedTabsDF containing the joined dataframe.
   */
  def fromMasterTimeSeriesDFPair(masterTabDF: RidershipMasterTabDF,
                                 timeSeriesDataTabDF: TimeSeriesDataTabDF): JoinedTabsDF = {

    // Filter and join the dataframes
    val joinedTabsDF = timeSeriesDataTabDF
      .dataframe()
      .join(masterTabDF.dataframe(), Seq("checksum"))

    // Create an instance of JoinedTabsDF and display the first row of the joined dataframe
    val joined = new JoinedTabsDF(joinedTabsDF)
      .withSanitizedColumns
    joined
  }

  /**
   * Helper function to sanitize column names:
   * Replaces spaces with underscores and converts to lowercase.
   *
   * @param name Original column name.
   * @return Sanitized column name.
   */
  def sanitizeColumnNames(name: String): String = {
    name.replaceAll("\\s+", "_").toLowerCase()
  }
}

/**
 * Represents a joined dataframe from RidershipMasterTabDF and TimeSeriesDataTabDF.
 *
 * @param df Underlying dataframe that this class wraps around.
 */
class JoinedTabsDF(df: DataFrame) {

  /**
   * Provides access to the underlying data frame.
   *
   * @return The underlying DataFrame.
   */
  def dataframe(): DataFrame = df

  def withSanitizedColumns: JoinedTabsDF = {
    val sanitized = df.columns.foldLeft(df) { (updatedDF, columnName) =>
      updatedDF.withColumnRenamed(columnName, JoinedTabsDF.sanitizeColumnNames(columnName))
    }
    new JoinedTabsDF(sanitized)
  }

}
