import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import org.apache.log4j.Logger
import org.apache.log4j.Level



object joinScala{
    def main(arrgs: Array[String]){
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        val conf = new SparkConf().setAppName("JoinScala").setMaster("local")
        val sc = new SparkContext(conf)
        val rawCustomers = sc.textFile("D:\\Python\\Project\\data\\Customers.csv")
        val rawProducts = sc.textFile("D:\\Python\\Project\\data\\Products.csv")
        val rawOrders = sc.textFile("D:\\Python\\Project\\data\\Orders.csv")
        val rawOrderDetails = sc.textFile("D:\\Python\\Project\\data\\OrderDetails.csv")
        val orderDetails = rawOrderDetails.map(l => parsOrderDetails(l.split(","))).filter(l=> l("ProductId").contains("61"))
        val orderDetailKeyValuePair = orderDetails.map(l => (l("ProductId"),l))
        val products = rawProducts.map(l => parsProducts(l.split(",")))
        val productsKeyValuePair = products.map(l => (l("ProductId"),l))
        val joinOrderDetails = orderDetailKeyValuePair.rightOuterJoin(productsKeyValuePair)
        joinOrderDetails.foreach(l => println(l))
        sc.stop()

    }
    def parsOrderDetails(attrsList :Array[String]) = {
        var orderDetail = Map[String, String]()
        orderDetail("OrderId") = attrsList(0)
        orderDetail("ProductId") = attrsList(1)
        orderDetail("Quanrity") = attrsList(2)
        orderDetail("UnitPrice") = attrsList(3)
        orderDetail
    }
    def parsProducts(attrsList :Array[String]) = {
        var product = Map[String, String]()
        product("ProductId") = attrsList(0)
        product("ProductName") = attrsList(1)
        product("ProductType") = attrsList(2)
        product("UnitPrice") = attrsList(3)
        product
    }

}
