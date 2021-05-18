@file:JvmName("RawPersonBirthDay1")

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.DataTypes
import org.jetbrains.kotlinx.spark.api.toDS
import org.jetbrains.kotlinx.spark.api.withSpark


data class RawPersonBirthDay(val name : String, val day : String, val month : String, val year : String)

var fixYear = udf(
    { year : String -> when(year.length){
        1 -> 2000 + year.toInt()
        2 -> 1900 + year.toInt()
        else -> year.toInt()
    } }, DataTypes.IntegerType
)!!


fun main() {

    val dataList = listOf(RawPersonBirthDay("Niko", "28", "1", "2002"),
        RawPersonBirthDay("Kuku", "23", "5", "81"), // 1981
        RawPersonBirthDay("John", "12", "12", "6"), // 2006
        RawPersonBirthDay("Rosy", "7", "8", "63"), //1963
        RawPersonBirthDay("Kuku", "23", "5", "81")) // 1981
    


    withSpark {
        spark.sql("set spark.sql.legacy.allowUntypedScalaUDF=true")

        spark.udf().register("fixYear",fixYear)

        val rawDataDF = spark.toDS(dataList).toDF("name", "day", "month", "year")
        val finalDF = rawDataDF.withColumn("id", monotonically_increasing_id()) // adding additional auto-generated column ID
            .withColumn("month",col("month").cast(DataTypes.IntegerType)) // casting col type to Integer
            .withColumn("day",col("day").cast(DataTypes.IntegerType))
            .withColumn("year", functions.callUDF("fixYear", col("year")))

        finalDF.show()

    }

}