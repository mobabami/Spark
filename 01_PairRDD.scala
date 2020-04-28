import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.Map

object PairRdd0_01 {
  def main(args: Array[String]) {
    println(System.getProperty("user.dir"))

    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val rawCustomers = sc.textFile("data\\Customers.csv")
    val rawProducts = sc.textFile("data\\Products.csv")
    val rawOrders = sc.textFile("data\\Orders.csv")
    val rawOrderDetails = sc.textFile("data\\OrderDetails.csv")
    
    val customers = rawCustomers.map(c => parseCustomer(c.split(",")))
    val customerKeyValuePair = customers.map(c => (c("CustomerId"), c))

    val outpuList = customerKeyValuePair.collectAsMap
    
    for(x <- outpuList)
      println(x)

    sc.stop()
  }

  def parseCustomer(attributeList :Array[String]) = {
    var customer = Map[String, String]()
    customer("CustomerId") = attributeList(0)
    customer("City") = attributeList(1)
    customer("Country") = attributeList(2)
    customer
  }
    

}