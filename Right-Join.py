from pyspark.sql import SparkSession
from decimal import Decimal
spark = SparkSession.builder.appName("northwind").master("local").getOrCreate()
sc = spark.sparkContext
rawCustomers = sc.textFile("data\\Customers.csv")
rawProducts = sc.textFile("data\\Products.csv")
rawOrders = sc.textFile("data\\Orders.csv")
rawOrderDetails = sc.textFile("data\\OrderDetails.csv")

def parsCustomers(attList):
    customer = {}
    customer["CustomerId"] = attList[0]
    customer["City"] = attList[1]
    customer["Country"] = attList[2]
    return customer

def parsProducts(attList):
    product = {}
    product["ProductId"] = attList[0]
    product["ProductName"] = attList[1]
    product["ProductType"] = attList[2]
    product["UnitPrice"] = attList[3]
    return product

def parsOrders(attList):
    order = {}
    order["OrderId"] = attList[0]
    order["CustomerId"] = attList[1]
    order["OrderDate"] = attList[2]
    return order

def parseOrderDetails(attList):
    orderDetail = {}
    orderDetail["OrderId"] = attList[0]
    orderDetail["ProductId"] = attList[1]
    orderDetail["Quantity"] = attList[2]
    orderDetail["UnitPrice"] = attList[3]
    return orderDetail

customers = rawCustomers.map(lambda l: parsCustomers(l.split(",")))
customersKeyValuePair = customers.map(lambda l: (l["CustomerId"],l))

products = rawProducts.map(lambda l: parsProducts(l.split(",")))
pruductsKeyValuePair = products.map(lambda l: (l["ProductId"],l))

orders = rawOrders.map(lambda l: parsOrders(l.split(",")))
ordersKeyValuePair = orders.map(lambda l: (l["OrderId"],l))

orderDetails = rawOrderDetails.map(lambda l: parseOrderDetails(l.split(","))).filter(lambda l: l["ProductId"] in "61")
orderDetailsKeyValuePair = orderDetails.map(lambda l: ( l["ProductId"], l) )

joinOrders = orderDetailsKeyValuePair.rightOuterJoin(pruductsKeyValuePair)
#joinOrders = orderDetailsKeyValuePair.cogroup(pruductsKeyValuePair)

joinOrders.foreach(lambda l: print(l))
