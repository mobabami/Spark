import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Shuffling_02{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val pairs = sc.parallelize(List((1, "one"), (2, "two"), (3, "three")))

    // println(pairs)
    // println(pairs.groupByKey())
    
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

    val result = purchaseRDD.map(c => (c.customerId, c.price)).groupByKey() // moves the data per key to each node 

    println("--------------------result 1")
    for(x <- result.collect())
     println(x)

    val result2 = result.map(c => (c._1, c._2.sum))
    println("--------------------result 2")
    for(x <- result2.collect())
     println(x)

    val result3 = result.map(c => (c._1, (c._2.size, c._2.sum)))
    println("--------------------result 3")
    for(x <- result3.collect())
     println(x)

    val speedResult = purchaseRDD.map(c => (c.customerId, (1, c.price)))
                                 .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
    // advantage is the reduction in nodes then shuffling the data

    println("--------------------result 4")
    for(x <- speedResult.collect())
      println(x)
  }
}
