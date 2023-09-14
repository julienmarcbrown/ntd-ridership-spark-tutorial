package com.ganzekarte.examples.etl.xls

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The `JoinedTabsDF` utility object offers functionality to merge dataframes
 * from both `RidershipMasterTabDF` and `TimeSeriesDataTabDF`. Furthermore, it ensures that the columns are named
 * consistently for downstream processing.
 */
object JoinedTabsDF {

  /**
   * Combines the dataframes from RidershipMasterTabDF and TimeSeriesDataTabDF based on the shared 'checksum' column.
   * The combined dataframe is sanitized to ensure column names follow a specific pattern: replacing spaces with
   * underscores and converting to lowercase.
   *
   * @param masterTabDF         An instance of RidershipMasterTabDF containing master data.
   * @param timeSeriesDataTabDF An instance of TimeSeriesDataTabDF holding time series data.
   * @return A new instance of JoinedTabsDF encapsulating the merged dataframe.
   */
  def fromMasterTimeSeriesDFPair(masterTabDF: RidershipMasterTabDF,
                                 timeSeriesDataTabDF: TimeSeriesDataTabDF): JoinedTabsDF = {

    // Joining the dataframes using the 'checksum' column.
    val joinedTabsDF = timeSeriesDataTabDF
      .dataframe()
      .join(masterTabDF.dataframe(), Seq("checksum"))

    // Sanitizing column names post-joining.
    new JoinedTabsDF(joinedTabsDF).withSanitizedColumns
  }

  /**
   * Transforms a given column name by replacing spaces with underscores and converting the entire string to lowercase.
   * This ensures consistency in column naming throughout the application.
   *
   * @param name A string representing the original column name.
   * @return A string reflecting the sanitized column name.
   */
  def sanitizeColumnNames(name: String): String = {
    name.replaceAll("\\s+", "_").toLowerCase()
  }
}

/**
 * `JoinedTabsDF` represents a dataframe resulting from the merge of `RidershipMasterTabDF` and `TimeSeriesDataTabDF`.
 * This class provides utility functions for operations specific to this merged dataframe.
 *
 * @param df A dataframe representing the merged data.
 */
class JoinedTabsDF(df: DataFrame) {

  /**
   * Provides access to the encapsulated dataframe.
   *
   * @return The underlying DataFrame.
   */
  def dataframe(): DataFrame = df

  /**
   * Returns a new instance of JoinedTabsDF where all column names are sanitized:
   * spaces are replaced with underscores and names are converted to lowercase.
   * This aids in standardizing the dataframe structure for future operations.
   *
   * @return A new instance of JoinedTabsDF with sanitized column names.
   */
  def withSanitizedColumns: JoinedTabsDF = {
    val sanitizedDF = df.columns.foldLeft(df) { (currentDF, columnName) =>
      currentDF.withColumnRenamed(columnName, JoinedTabsDF.sanitizeColumnNames(columnName))
    }
    new JoinedTabsDF(sanitizedDF)
  }
}
