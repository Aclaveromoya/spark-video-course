package es.azaharaclavero.sparkvideocourse
package utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/**
 * Trait `SparkConfiguration` provides a centralized configuration for creating a `SparkSession`.
 *
 * - Uses the Typesafe Config library to load application settings from `application.conf`.
 * - Configures the `SparkSession` with the application name and master URL specified in the configuration file.
 * - Ensures a single instance of `SparkSession` is available for use across the application.
 */
trait SparkConfiguration {
  private val config = ConfigFactory.load()

  val spark: SparkSession = SparkSession.builder()
    .appName(config.getString("spark.appName"))
    .master(config.getString("spark.master"))
    .getOrCreate()
}
