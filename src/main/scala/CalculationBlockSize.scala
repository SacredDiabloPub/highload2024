package hightload2024

import CalculationTime.measureOnlyTime
import org.apache.orc.OrcFile
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.parallel.CollectionConverters.ArrayIsParallelizable

/**
 * Если есть сильное ограничение по памяти - используйте rawSize, в остальных случаях - compressSize
 *
 * @param rawSize                Размер данных в не сжатом виде
 * @param compressSize           Размер данных в сжатом виде
 * @param rowNum                 Число строк
 * @param averageCompressRowSize Средний размер строки в сжатом виде
 * @param calculateBlockCount    Расчётное число записей для репартицирования
 */
case class DataFrameInfo(rawSize: Long,
                         compressSize: Long,
                         rowNum: Long,
                         averageCompressRowSize: Long,
                         calculateBlockCount: Int)

object CalculationBlockSize {

  /**
   * Метод для вычисления оптимального размера блока. Расчёт на основе input_file_name.
   *
   * @param df              Входной DataFrame, который будет разбиваться на блоки.
   * @param repartitionSize Размер блока к которому стремиться.
   * @param spark           SparkSession, необходимый для работы с Hadoop Configuration и другими конфигурациями Spark.
   * @return
   */
  def blockSize(df: DataFrame, repartitionSize: Int)(implicit spark: SparkSession): DataFrameInfo = {
    val conf = spark.sparkContext.hadoopConfiguration
    val files = df.select(input_file_name()).distinct().collect.map(row => row.getString(0))
    val contentSize = files.par.map(file => {
      val content = OrcFile.createReader(new org.apache.hadoop.fs.Path(file), OrcFile.readerOptions(conf))
      content.close()
      (content.getRawDataSize, content.getContentLength, content.getNumberOfRows)
    }).reduce((c, c1) => (c._1 + c1._1, c._2 + c1._2, c._3 + c1._3))

    DataFrameInfo(contentSize._1, contentSize._2, contentSize._3,
      (contentSize._2 / contentSize._3) * 2, ((contentSize._1 / 1024.0 / 1024) / repartitionSize + 1).toInt)
  }

  /**
   * Метод для вычисления оптимального размера блока. Расчёт на основе inputFile.
   *
   * @param df              Входной DataFrame, который будет разбиваться на блоки.
   * @param repartitionSize Размер блока к которому стремиться.
   * @param spark           SparkSession, необходимый для работы с Hadoop Configuration и другими конфигурациями Spark.
   * @return
   */
  def blockSizeByFile(df: DataFrame, repartitionSize: Int)(implicit spark: SparkSession): DataFrameInfo = {
    val conf = spark.sparkContext.hadoopConfiguration
    val files = df.inputFiles
    val contentSize = files.par.map(file => {
      val content = OrcFile.createReader(new org.apache.hadoop.fs.Path(file), OrcFile.readerOptions(conf))
      content.close()
      (content.getRawDataSize, content.getContentLength, content.getNumberOfRows)
    }).reduce((c, c1) => (c._1 + c1._1, c._2 + c1._2, c._3 + c1._3))
    DataFrameInfo(contentSize._1, contentSize._2, contentSize._3,
      (contentSize._2 / contentSize._3) * 2, ((contentSize._1 / 1024.0 / 1024) / repartitionSize + 1).toInt)
  }

  /**
   * Метод для нахождения оптимального размера блока DataFrame после его репликации.
   *
   * @param df               Входной DataFrame, который будет разбиваться на блоки.
   * @param savePath         Путь сохранения данных в формате ORC.
   * @param starterBlockSize Начальный размер блока в байтах (по умолчанию 134217728 байт).
   * @param step             Шаг изменения размера блока (по умолчанию 2).
   * @param time             Максимальное время выполнения операции (по умолчанию Long.MaxValue).
   * @param iterations       Количество итераций для поиска оптимального размера блока (по умолчанию 10).
   * @param minTime          Текущее минимальное время выполнения операции (по умолчанию Long.MaxValue).
   * @param bestBlockSize    Лучший найденный размер блока на данный момент (по умолчанию 134217728 байт).
   * @param bestRepartSize   Лучшее найденное количество партиций на данный момент (по умолчанию 200).
   * @param trend            Флаг, определяющий тенденцию изменения размера блока (true - увеличение, false - уменьшение) (по умолчанию true).
   * @param isBig            Логический флаг, указывающий на размер данных в DataFrame (по умолчанию true).
   * @param spark            SparkSession, необходимый для работы с Hadoop Configuration и другими конфигурациями Spark.
   * @return Возвращает кортеж (Long, Long, Long, Int), где первый элемент - текущий размер блока в байтах, второй элемент - минимальное время выполнения операции, третий элемент - лучший найденный размер блока, четвертый элемент - лучшее количество партиций.
   */
  def findOptimalBlockSize(
                            df: DataFrame,
                            savePath: String,
                            starterBlockSize: Long = 134217728,
                            step: Double = 2.0,
                            time: Long = Long.MaxValue,
                            iterations: Int = 10,
                            minTime: Long = Long.MaxValue,
                            bestBlockSize: Long = 134217728,
                            bestRepartSize: Int = 200,
                            trend: Boolean = true
                          )(isBig: Boolean = true)(implicit spark: SparkSession): (Long, Long, Long, Int) = {
    spark.catalog.clearCache()
    spark.conf.set("spark.sql.files.maxPartitionBytes", 134217728)
    spark.conf.set("spark.sql.shuffle.partitions", 200)

    val div = (starterBlockSize / 1024.0 / 1024 + 1).toInt
    val contentSize = if (isBig) blockSize(df, div)
    else blockSizeByFile(df, div)

    val repartSize = (contentSize.compressSize / 1024.0 / 1024 / div).toInt + 1
    // Если у вас много маленьких файлов и нет больших (при использовании compressSize)
    // можно отказаться от настройки maxPartitionBytes
    spark.conf.set("spark.sql.files.maxPartitionBytes", starterBlockSize)
    spark.conf.set("spark.sql.shuffle.partitions", repartSize)

    // Измеряем время чтения данных
    val newTime = measureOnlyTime(
      // Можно проверять и без repartition
      df.repartition(repartSize)
        .write.format("orc")
        .mode("append").save(savePath))

    // Обновляем минимальное время и связанные с ним размеры
    val newCurrentMinTime = math.min(minTime, newTime)
    val (newBestBlockSize, newBestRepartSize) = if (newTime < minTime) {
      (starterBlockSize, repartSize)
    } else {
      (bestBlockSize, bestRepartSize)
    }

    println(
      s"""
         |contentSize = $contentSize
         |time = $time
         |newTime = $newTime
         |currentMinTime = $newCurrentMinTime
         |iterations = $iterations
         |minTime = $minTime
         |div = $div
         |step = $step
         |starterBlockSize = $starterBlockSize
         |repartSize = $repartSize
         |contentSize = $contentSize
         |bestBlockSize = $newBestBlockSize
         |bestRepartSize = $newBestRepartSize
         |
         |""".stripMargin)

    // Если достигли максимального количества итераций или стартовый блок стал слишком малым,
    // возвращаем текущий размер блока и минимальное время
    if (iterations <= 0 || starterBlockSize < 1024 * 1024 || step < 1) { // Минимальный размер блока — 1 МБ
      (starterBlockSize, newCurrentMinTime, newBestBlockSize, newBestRepartSize)
    } else {
      val nextIterations = iterations - 1

      if (newTime < time && trend) {
        // Время уменьшилось при увеличении размера блока — продолжаем увеличивать
        println("Увеличиваем starterBlockSize: newTime < time && trend")
        println(s"$newTime < $time && $trend")
        findOptimalBlockSize(
          df, savePath, math.max((starterBlockSize * step).toLong, 1024 * 1024), step,
          newTime, nextIterations, newCurrentMinTime, newBestBlockSize, newBestRepartSize, trend
        )(isBig)
      } else if (newTime > time && trend) {
        // Время увеличилось при увеличении размера блока — меняем направление и уменьшаем шаг
        println("Уменьшаем starterBlockSize и меняем направление: newTime > time && trend")
        println(s"$newTime > $time && $trend")
        findOptimalBlockSize(
          df, savePath, math.max((starterBlockSize / (step - 0.1)).toLong, 1024 * 1024), step - 0.1,
          newTime, nextIterations, newCurrentMinTime, newBestBlockSize, newBestRepartSize, !trend
        )(isBig)
      } else if (newTime < time && !trend) {
        // Время уменьшилось при уменьшении размера блока — продолжаем уменьшать
        println("Уменьшаем starterBlockSize: newTime < time && !trend")
        println(s"$newTime < $time && $trend")
        findOptimalBlockSize(
          df, savePath, math.max((starterBlockSize / step).toLong, 1024 * 1024), step,
          newTime, nextIterations, newCurrentMinTime, newBestBlockSize, newBestRepartSize, trend
        )(isBig)
      } else {
        // Время увеличилось при уменьшении размера блока — меняем направление и уменьшаем шаг
        println("Увеличиваем starterBlockSize и меняем направление: newTime > time && !trend")
        println(s"$newTime > $time && $trend")
        findOptimalBlockSize(
          df, savePath, math.max((starterBlockSize * (step - 0.1)).toLong, 1024 * 1024), step - 0.1,
          newTime, nextIterations, newCurrentMinTime, newBestBlockSize, newBestRepartSize, !trend
        )(isBig)
      }
    }
  }
}
