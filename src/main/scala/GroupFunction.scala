package hightload2024

import hightload2024.CalculationBlockSize.blockSize
import hightload2024.CalculationTime.averageTime
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object GroupFunction {

  def groupTestResult(df: DataFrame, sizeForAuto: Int, customBestShuffle: Int, customBestReadBytes: Long)
                     (groupCols: List[Column], expression: List[String])
                     (implicit spark: SparkSession) = {
    val valuePartStandart = averageTime(partBy(df, 200, groupCols, expression), 10)
    val valueRepart400 = averageTime(partRepart(df, 400, groupCols, expression), 10)
    val valuePart400 = averageTime(partBy(df, 400, groupCols, expression), 10)
    val valueRepart10 = averageTime(partRepart(df, 10, groupCols, expression), 10)
    val valuePart10 = averageTime(partRepart(df, 10, groupCols, expression), 10)
    val valueCalculate = averageTime(partCalculate(df, sizeForAuto, groupCols, expression), 10)
    val valueCalculateRepart = averageTime(partCalculateRepart(df, sizeForAuto, groupCols, expression), 10)
    val valuePartBestParams = averageTime(partBestParams(df, groupCols, expression)(customBestShuffle, customBestReadBytes), 10)

    val resultAGG =
      s"""
         |valuePartStandart = $valuePartStandart
         |valueRepart400 = $valueRepart400
         |valuePart400 = $valuePart400
         |valueRepart10 = $valueRepart10
         |valuePart10 = $valuePart10
         |valueCalculate = $valueCalculate
         |valueCalculateRepart = $valueCalculateRepart
         |valuePartBestParams = $valuePartBestParams
         |""".stripMargin

    println(resultAGG)
  }

  /**
   * Группировка без изменений
   */
  def partBy(df: DataFrame, part: Int, groupCols: List[Column], expression: List[String])(implicit spark: SparkSession) = {
    spark.catalog.clearCache()

    spark.conf.set("spark.sql.shuffle.partitions", part)

    val ex = expression.map(e => expr(e))
    val d = df
    println(d.groupBy(groupCols:_*).agg(ex.head, ex.tail:_*).persist().count())
  }

  /**
   * Группировка с исскуственным шафлом 10
   * @param df
   */
  def partRepart(df: DataFrame, repart: Int, groupCols: List[Column], expression: List[String])
                (implicit spark: SparkSession) = {
    spark.catalog.clearCache()

    spark.conf.set("spark.sql.shuffle.partitions", repart)
    val ex = expression.map(e => expr(e))

    val d = df.repartition(repart)
    println(d.groupBy(groupCols:_*).agg(ex.head, ex.tail:_*).persist().count())
  }

  /**
   * Группировка с автоопределением числа партиций для шафла конфигурацией
   * @param df
   */
  def partCalculate(df: DataFrame, _blockSize: Int, groupCols: List[Column], expression: List[String])
                   (implicit spark: SparkSession) = {
    spark.catalog.clearCache()

    val bc = blockSize(df, _blockSize)

    spark.conf.set("spark.sql.shuffle.partitions", bc.calculateBlockCount)
    spark.conf.set("spark.sql.files.maxPartitionBytes", bc.averageCompressRowSize * 250000)
    val ex = expression.map(e => expr(e))

    val d = df

    println(d.groupBy(groupCols:_*).agg(ex.head, ex.tail:_*).persist().count())
  }

  def partCalculateRepart(df: DataFrame, _blockSize: Int, groupCols: List[Column], expression: List[String])
                         (implicit spark: SparkSession) = {
    spark.catalog.clearCache()

    val bc = blockSize(df, _blockSize)

    spark.conf.set("spark.sql.shuffle.partitions", bc.calculateBlockCount)
    spark.conf.set("spark.sql.files.maxPartitionBytes", bc.averageCompressRowSize * 250000)

    println(s"Size block1 $bc")
    val ex = expression.map(e => expr(e))

    val d = df.repartition(bc.calculateBlockCount)

    println(d.groupBy(col("inn_src")).agg(count(col("num_a")), avg(col("num_b")))
      .persist().count())
  }

  def partBestParams(df: DataFrame, groupCols: List[Column], expression: List[String])(repart: Int, blockSize: Long)
                    (implicit spark: SparkSession) = {
    spark.catalog.clearCache()

    spark.conf.set("spark.sql.shuffle.partitions", repart)
    spark.conf.set("spark.sql.files.maxPartitionBytes", blockSize)
    val ex = expression.map(e => expr(e))

    val d = df

    println(d.groupBy(groupCols:_*).agg(ex.head, ex.tail:_*).persist().count())
  }


}
