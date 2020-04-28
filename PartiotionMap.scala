import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PartitionMap{
    def main(argus: Array[String]){
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf= new SparkConf().setAppName("PartitionMap").setMaster("local")
    val sc = new SparkContext(conf)
    val ls= List("yellow","red","blue","cyan","black","green")
    val rddinit = sc.parallelize(ls,3)
    val rddMapped = rddinit.mapPartitionsWithIndex(
        (index,iterator) => {
            println("Called in partition -> " + index)
            val rddlst = iterator.toList
            rddlst.map(x => x + "->" + index).iterator
        }
    )
    print(rddMapped.collect())
    }
}