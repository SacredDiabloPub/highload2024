package hightload2024

import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._

object CalculationTime {
  def measureOnlyTime[T](block: => T): Long = {
    val startTime = System.nanoTime()
    block
    val endTime = System.nanoTime()
    endTime - startTime
  }


  def averageTime[T](block: => T, runs: Int)
                    (implicit spark: SparkSession): String = {
    require(runs > 0, "Number of runs must be greater than 0")

    var minDuration = Long.MaxValue
    var maxDuration = Long.MinValue

    val totalDuration = (1 to runs).map(n => {
      spark.catalog.clearCache()
      val mot = measureOnlyTime(block)
      println(s"$n = $mot nanos")

      if (mot < minDuration) minDuration = mot
      if (mot > maxDuration) maxDuration = mot

      mot
    }).sum

    val averageDuration = totalDuration / runs
    println(s"Average: ${averageDuration.nanos.toMillis} ms, Min: ${minDuration.nanos.toMillis} ms, Max: ${maxDuration.nanos.toMillis} ms")
    s"Average: ${averageDuration.nanos.toMillis} ms, Min: ${minDuration.nanos.toMillis} ms, Max: ${maxDuration.nanos.toMillis} ms"
  }
}
