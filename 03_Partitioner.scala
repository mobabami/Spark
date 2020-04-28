import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import org.apache.spark.RangePartitioner
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Partitioner_03{
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    case class CustomerPurchase(customerId: Int, destination: String, price: Double)

    val dataSet = List(
                  CustomerPurchase(100, "Genve", 22),
                  CustomerPurchase(300, "Zurikh", 42),
                  CustomerPurchase(100, "Friborg", 12),
                  CustomerPurchase(200, "Munich", 8),
                  CustomerPurchase(100, "Lucern", 31),
                  CustomerPurchase(300, "Basel", 60)
    )
    val purchaseRDD = sc.parallelize(dataSet)

    val pairedPurchase = purchaseRDD.map(x => (x.customerId, x.price))

    val tunedPartition = new RangePartitioner(4, pairedPurchase)  //spark will sample to find best ranges

    val partitionedData = pairedPurchase.partitionBy(tunedPartition).persist() //will do actual assignment and IMPORTANT to call persist to prevent further shuffling (by possible reevaluation) 


    println(pairedPurchase.count())

  }
}
