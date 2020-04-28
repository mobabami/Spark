from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Northwind").master("local").getOrCreate()
sc = spark.sparkContext

rawCustomers = sc.textFile("data\\Customers.csv")
rawProducts = sc.textFile("data\\Products.csv")
rawOrders = sc.textFile("data\\Orders.csv")
rawOrderDetails = sc.textFile("data\\OrderDetails.csv")


rawCustomers.collect()
rawProducts.collect()
rawOrders.collect()
rawOrderDetails.collect()

def parseCustomer(attList):
    customer = {}
    customer["CustomerId"] = attList[0]
    customer["City"] = attList[1]
    customer["Country"] = attList[2]
    return customer

def func(l):
    return parseCustomer(l.split(","))

customers = rawCustomers.map(func)
customerKeyValuePair = customers.map(lambda l: (l["CustomerId"], l) )

customerKeyValuePair.foreach(lambda l: print(l))
