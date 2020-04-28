import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._

object project_df {
    def main(args: Array[String]){
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        val conf = new SparkConf().setAppName("Project DF")
        conf.setMaster("local")
        val sc = new SparkContext(conf)
        val sqlcon = new SQLContext(sc)
        val sensorDF = sqlcon.read.json("D:\\Python\\Project\\data\\source.json")
        //sensorDF.show()
        //sensorDF.filter("input == 'sensor_9'").show()
        sensorDF.groupBy("date").agg(avg("value")).show()
    
    }
}