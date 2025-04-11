package es.azaharaclavero.sparkvideocourse
package utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Trait `SparkUtils` provides utility methods for working with Spark
 */
trait SparkUtils {
  private val config = ConfigFactory.load()
  private val csvPath = config.getString("spark.employeesPath")

  /**
   * Reads a CSV file into a DataFrame.
   *
   * @param spark The active SparkSession.
   * @param path The path to the CSV file.
   * @param header Whether the CSV file contains a header row (default: true).
   * @param inferSchema Whether to infer the schema of the CSV file (default: true).
   * @return A DataFrame containing the data from the CSV file.
   */
  def readCVS(spark:SparkSession, path:String, header:Boolean = true, inferSchema: Boolean = true) : DataFrame = {
    spark.read
      .option("header", header.toString)
      .option("inferSchema", inferSchema.toString)
      .csv(csvPath)
  }

}
