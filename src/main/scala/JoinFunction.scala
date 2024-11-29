package hightload2024

import hightload2024.CalculationBlockSize.{blockSize, blockSizeByFile}
import hightload2024.CalculationTime.averageTime
import hightload2024.Presentation.{standartFileSize, standartShuffle}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * В настройка по умолчанию чтение происходит ORC файлов
 */
object JoinFunction extends Logging {

  def joinTestResult(pathDf1: String, pathDf2: String, savePath: String, filterDf1Condition: Column,
                      filterDf2Condition: Column, joinCondition: Column, joinType: String)
                     (outputCol: List[String])
                     (sizeDf1: Int = 0, sizeDf2: Int = 0)
                     (customBestReadBytesDf1: Long = 0, customBestReadBytesDf2: Long = 0, customBestShuffle: Int = 0)
                     (implicit spark: SparkSession) = {
    /**
     * Инициализация, чтобы дальше время на подключение к фс не влияло на оценку
     */
    val df1 = spark.read.orc(pathDf1).filter(filterDf1Condition)
    val df2 = spark.read.orc(pathDf2).filter(filterDf2Condition)

    df1.as("df1").join(df2.as("df2"), joinCondition, joinType)
      .select(outputCol.map(col): _*)
      .write.mode("append")
      .format("orc").save(savePath)

    val standardJoin = averageTime(joinStandard(pathDf1, pathDf2, savePath, filterDf1Condition, filterDf2Condition,
      joinCondition, joinType)(outputCol), 5)
    val autoRepartJoin = averageTime(joinAutoRepartition(pathDf1, pathDf2, savePath, filterDf1Condition, filterDf2Condition,
      joinCondition, joinType)(outputCol)(sizeDf1, sizeDf2)(true), 5)
    val customRepartJoin = averageTime(joinRepartitionCustom(pathDf1, pathDf2, savePath, filterDf1Condition, filterDf2Condition,
        joinCondition, joinType)(outputCol)(customBestReadBytesDf1, customBestReadBytesDf2, customBestShuffle), 5)

    val resultJoin =
      s"""
         |autoRepartJoin = $autoRepartJoin
         |customRepartJoin = $customRepartJoin
         |standardJoin = $standardJoin
         |""".stripMargin

    println(resultJoin)
  }
  def joinStandard(pathDf1: String, pathDf2: String, savePath: String,
                   filterDf1Condition: Column, filterDf2Condition: Column,
                   joinCondition: Column, joinType: String)(
                  outputCol: List[String])(implicit spark: SparkSession) = {
    spark.conf.set("spark.sql.shuffle.partitions", standartShuffle)
    spark.conf.set("spark.sql.files.maxPartitionBytes", standartFileSize)

    val df1 = spark.read.orc(pathDf1).filter(filterDf1Condition)

    val df2 = spark.read.orc(pathDf2).filter(filterDf2Condition)

    df1.as("df1").join(df2.as("df2"), joinCondition, joinType)
      .select(outputCol.map(col): _*)
      .write.mode("append")
      .format("orc").save(savePath)
  }

  def joinAutoRepartition(pathDf1: String, pathDf2: String, savePath: String,
                          filterDf1Condition: Column, filterDf2Condition: Column,
                          joinCondition: Column, joinType: String)(
                           outputCol: List[String])(sizeDf1: Int, sizeDf2: Int)(isBig: Boolean)(
    implicit spark: SparkSession) = {
    spark.conf.set("spark.sql.shuffle.partitions", standartShuffle)
    spark.conf.set("spark.sql.files.maxPartitionBytes", standartFileSize)

    val rep1 = if (isBig) blockSize(spark.read.orc(pathDf1).filter(filterDf1Condition), sizeDf1)
    else blockSizeByFile(spark.read.orc(pathDf1).filter(filterDf1Condition), sizeDf1)
    val rep2 = if (isBig) blockSize(spark.read.orc(pathDf2).filter(filterDf2Condition), sizeDf2)
    else blockSizeByFile(spark.read.orc(pathDf2).filter(filterDf2Condition), sizeDf2)

    spark.conf.set("spark.sql.shuffle.partitions", List(rep1.calculateBlockCount, rep2.calculateBlockCount).max)

      // Можно делать расчёт размера делается исходя из размера строки * на желаемое число строк
     // spark.conf.set("spark.sql.files.maxPartitionBytes", 250000 * rep1.averageCompressRowSize)
    // Задавая параметр перед чтением датафрейма - мы читаем df с этой настройкой
    spark.conf.set("spark.sql.files.maxPartitionBytes", sizeDf1 * 1024 * 1024)
    val df1 = spark.read.orc(pathDf1).filter(filterDf1Condition)

    spark.conf.set("spark.sql.files.maxPartitionBytes", sizeDf2 * 1024 * 1024)
    val df2 = spark.read.orc(pathDf2).filter(filterDf2Condition)

    df1.as("df1").join(df2.as("df2"), joinCondition, joinType)
      .select(outputCol.map(col): _*)
      // Сюда можно добавить репартицирование перед сохранением на основе данных по предыдущему дню
      .write.mode("append")
      .format("orc").save(savePath)
  }

  def   joinRepartitionCustom(pathDf1: String, pathDf2: String, savePath: String,
                            filterDf1Condition: Column, filterDf2Condition: Column,
                            joinCondition: Column, joinType: String)(
                             outputCol: List[String])(
    customBestReadBytesDf1: Long, customBestReadBytesDf2: Long, customBestShuffle: Int)(implicit spark: SparkSession) = {

    spark.conf.set("spark.sql.files.maxPartitionBytes", customBestReadBytesDf1)
    val df1 = spark.read.orc(pathDf1).filter(filterDf1Condition)

    spark.conf.set("spark.sql.files.maxPartitionBytes", customBestReadBytesDf2)
    val df2 = spark.read.orc(pathDf2).filter(filterDf2Condition)

    spark.conf.set("spark.sql.shuffle.partitions", customBestShuffle)

    df1.as("df1").join(df2.as("df2"), joinCondition, joinType)
      .select(outputCol.map(col): _*)
      // Сюда можно добавить репартицирование перед сохранением на основе данных по предыдущему дню
      .write.mode("append")
      .format("orc").save(savePath)
  }
}
