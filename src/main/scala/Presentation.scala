package hightload2024

import hightload2024.CalculationBlockSize.findOptimalBlockSize
import hightload2024.GroupFunction._
import hightload2024.JoinFunction.joinTestResult
import hightload2024.WindowFunction.addCol
import org.apache.spark.sql.functions.{col, concat, expr, rand}

object Presentation extends SparkApp {

  val standartShuffle = spark.conf.get("spark.sql.shuffle.partitions")
  val standartFileSize = spark.conf.get("spark.sql.files.maxPartitionBytes")

  val pathDf1 = "path"
  val pathDf2 = "path2"
  val savePath = "savePath"
  val sizeDf1 = "sizeDf1"
  val sizeDf2 = "sizeDf2"



  /** Example
   * Функции реализованы под мои задачи - и их может быть необходимо оптимизировать под ваши условия =)
   * findOptimalBlockSize - рекомендую прогонять блоками по 5-10 запусков несколько раз в течении дня
   findOptimalBlockSize(spark.read.orc(pathDf1).filter(col("a").isNotNull && col("b").isNotNull), savePath)(false)
   findOptimalBlockSize(spark.read.orc(pathDf1), savePath)(true)
  groupTestResult(spark.read.orc(pathDf1).filter(col("a").isNotNull && col("b").isNotNull), sizeDf1.toInt, 156, 371536962)(
    List(col("a")), List("count(*)", "sum(b)"))
  joinTestResult(pathDf1, pathDf2, savePath,
    col("a").isNotNull && col("b").isNotNull,
    col("a").isNotNull && col("b").isNotNull,
    col("a") === col("a") && col("b") === col("b") && expr("df1.c BETWEEN df2.c AND df2.d"),
    "inner")(List("df1.*"))(sizeDf1.toInt, sizeDf2.toInt)(268435456, 268435456, 294)
  addCol(spark.read.orc(pathDf1), savePath, col("a"), List(col("b"), col("c")))
  */
}
