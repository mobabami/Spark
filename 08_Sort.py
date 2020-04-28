from pyspark.sql import SparkSession
from itertools import islice
from decimal import Decimal


spark = SparkSession.builder.appName("Northwind").master("local").getOrCreate()
sc = spark.sparkContext

rawCustomers = sc.textFile(r"data\Customers.csv")
rawProducts = sc.textFile(r"data\Products.csv")
rawOrders = sc.textFile(r"data\Orders.csv")
rawOrderDetails = sc.textFile(r"data\OrderDetails.csv")

def parseCustomer(attributeList):
    customer = {}
    customer["CustomerId"] = attributeList[0]
    customer["City"] = attributeList[1]
    customer["Country"] = attributeList[2]
    return customer


customers = rawCustomers.map(lambda l: parseCustomer(l.split(",")))
customerKeyValuePair = customers.map(lambda c: (c["CustomerId"], c))


print("\nSortBy function\n")
for i in customerKeyValuePair.sortBy(lambda l: l[1]["City"], False).take(10):
    print(i)

print("\nSort By Key\n")
for i in customerKeyValuePair.sortByKey().take(10):
    print(i)

print("\nSort By Key with function\n")
for i in customerKeyValuePair.sortByKey(False, 1, lambda l: l[2]).take(10):
    print(i)