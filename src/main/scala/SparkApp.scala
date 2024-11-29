package hightload2024

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

trait SparkApp extends App with Logging{
  implicit val spark: SparkSession = SparkSession
    .builder()
    .getOrCreate()
}
