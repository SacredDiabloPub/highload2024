package hightload2024

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.Try

object WindowFunction {
  def winAvg(df: DataFrame, path: String, cols: Column)(implicit spark: SparkSession) = {
    df.withColumn("avg", avg(cols).over())
      .write.format("orc").mode("append")
      .save(path)
  }

  def winCountBy(df: DataFrame, path: String, cols: Column, groupCol: List[Column])(implicit spark: SparkSession) = {
    df.withColumn("cnt", count(cols)
      .over(Window.partitionBy(groupCol:_*)))
      .write.format("orc").mode("append")
      .save(path)
  }

  def winAvgWithParams(df: DataFrame, path: String, cols: Column)(implicit spark: SparkSession) = {
    spark.conf.set("spark.sql.windowExec.buffer.spill.threshold", 2097152)
    spark.conf.set("spark.sql.windowExec.buffer.in.memory.threshold", 2097152)

    df.withColumn("avg", avg(cols).over())
      .write.format("orc").mode("append")
      .save(path)
  }
  def winCountByWithParams(df: DataFrame, path: String, cols: Column, groupCol: List[Column])(implicit spark: SparkSession) = {
    spark.conf.set("spark.sql.windowExec.buffer.spill.threshold", 2097152)
    spark.conf.set("spark.sql.windowExec.buffer.in.memory.threshold", 2097152)
    df.withColumn("cnt", count(cols).over(Window.partitionBy(groupCol:_*)))
      .write.format("orc").mode("append")
      .save(path)
  }

  // Функция для 2 колонок группировки и 1 ключа
  def add(map: Map[(Long, Long), Long]): UserDefinedFunction = udf((col: Long, col2: Long) =>
    Try(map.get((col, col2))).getOrElse(null.asInstanceOf[Long]))

  def addCol(df: DataFrame, path: String, cols: Column, groupCol: List[Column])(implicit spark: SparkSession) = {
    // Для универсализации используется номера колонок, лучше использовать row.getAs[Type](colName)
    val m = spark.sparkContext.broadcast(df.groupBy(groupCol:_*).agg(cols)
      .collect().map(row => ((row.getAs[Long](0), row.getAs[Long](1)) -> row.getAs[Long](2))).toMap
    )

    df.withColumn("cnt", add(m.value)(groupCol:_*))
      .write.format("orc").mode("append")
      .save(path)
  }
}
