import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object project_sql{
    def main(args: Array[String]) {
        val spark = SparkSession.builder().appName("project sql basics").master("local").getOrCreate()
        
        val sensor_schema = StructType( Array(
            StructField("date", StringType, false),  
            StructField("time", StringType, false),
            StructField("input", StringType, false),
            StructField("value", DoubleType, false)
        ) )
        val sensoryData = spark.read.json("source.jsonl")

        sensoryData.show()

        println(sensoryData.schema)

        sensoryData.printSchema()

    }
}