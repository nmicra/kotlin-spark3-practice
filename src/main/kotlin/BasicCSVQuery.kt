

@file:JvmName("BasicCSVQuery")
import org.apache.spark.SparkConf
import org.jetbrains.kotlinx.spark.api.SparkSession

fun main() {


    val conf = SparkConf()
    conf.set("spark.app.name", "Hi there!")
    conf.set("spark.master", "local[3]")
    val spark = SparkSession.builder().config(conf).orCreate
    println("version >>> ${spark.version()}")
    val surveyDF = spark.read()
        .option("header",true)
        .option("inferSchema",true)
        .csv("data\\sample.csv")
        .repartition(2) //Just simulate that file was read from 2 different partitions

    surveyDF.show()
    // --------------------------

    val countDF = surveyDF.where("Age > 40")
        .select("Age","Gender", "Country","state")
        .groupBy("Country")
        .count()

    countDF.show()


}