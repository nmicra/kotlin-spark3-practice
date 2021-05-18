@file:JvmName("ApacheLogsAnalyze")
import org.jetbrains.kotlinx.spark.api.*
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.*


data class ApacheLogRecord(val ip: String, val date: String, val request: String, val referrer: String)
fun main() {



    val myReg = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".toRegex()



    withSpark {
        val logsDF = spark.read().textFile("data\\apache_logs.txt").toDF()

        val transformedDF = logsDF.map{
            val (ip, client, user, date, cmd, request, proto, status, bytes, referrer) = myReg.find(it.getString(0))!!.destructured
            ApacheLogRecord(ip, date, request, referrer)
        }

        transformedDF
            .withColumn("referrer",substring_index(col("referrer"), "/", 3))
            .groupBy("referrer").count().show(false)
    }

}