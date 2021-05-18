
/* SimpleApp.kt */
@file:JvmName("SimpleApp")
import org.apache.spark.SparkConf
import org.jetbrains.kotlinx.spark.api.*

fun main() {
    val logFile = "C:\\spark-3.1.1-bin-hadoop2.7\\README.md" // Change to your Spark Home path
    withSpark {
        spark.read().textFile(logFile).withCached {
            val numAs = filter { it.contains("a") }.count()
            val numBs = filter { it.contains("b") }.count()
            println("Lines with a: $numAs, lines with b: $numBs")
        }
    }

}