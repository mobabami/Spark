from pyspark.sql import SparkSession
from decimal import Decimal

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

def parsProducts(attList):
    product = {}
    product["ProductId"] = attList[0]
    product["ProductName"] = attList[1]
    product["ProductType"] = attList[2]
    product["UnitPrice"] = attList[3]
    return product

def parseOrder(attList):
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


def func(l):
    return parseCustomer(l.split(","))

customers = rawCustomers.map(func)
customerKeyValuePair = customers.map(lambda l: (l["CustomerId"], l) )

products = rawProducts.map(lambda l: parsProducts(l.split(",")))
productsKeyValuePair = products.map(lambda l: (l["ProductId"],l))

orders = rawOrders.map(lambda l: parseOrder(l.split(",")))
ordersKeyValuePair = orders.map(lambda l: ( l["OrderId"], l) )

orderDetails = rawOrderDetails.map(lambda l: parseOrderDetails(l.split(",")))
orderDetailsKeyValuePair = orderDetails.map(lambda l: ( l["OrderId"], l) )

def flattenProduct(p):
    plist = []
    for i in p["ProductType"].split("/"):
        np = p.copy()
        np["ProductType"] = i
        plist.append(np)
    return plist

print(products.count())
expanddProduc = products.flatMap(flattenProduct)
expanddProduc.foreach(lambda l: print(l))
print(expanddProduc.count())

# We should use aggregate because it is one operation
# result = orderDetails.fold(0, lambda l,v: l["UnitPrice"] + v["UnitPrice"])
result = orderDetails.map(lambda l: Decimal(l["UnitPrice"])).reduce(lambda f,s: f+s)
print("Mapped and reduce")
print(result)
result = orderDetails.aggregate(Decimal(0), lambda a,c: a + (Decimal(c["UnitPrice"])), lambda f,s: f+s )
print("Aggregate")
print(result)

# productsKeyValuePair.reduceByKey    <--- transformation

joinedOrders  = customerKeyValuePair.join( orders.map(lambda l: ( l["CustomerId"], l) ) )

total = orderDetails.map(lambda l: Decimal(l["Quantity"]) * Decimal(l["UnitPrice"]) ).reduce(lambda f,s: f+s)

print(total)

total = orderDetails.aggregate(0, lambda a,c: a + (Decimal(c["Quantity"]) * Decimal(c["UnitPrice"])), lambda f,s: f+s )

print(total)


# cogroup  = full outer join
myrdd1 = sc.parallelize([(1, "physics"),(2,"sanskrit"),(3,"hindi"),(4,"ohysical"),(6,"computer")])

myrdd2 = sc.parallelize([(4,"english"),(5,"arts"),(6,"social"),(1,"bio"),(1,"chemistry"), (1, "science")])

result = myrdd1.cogroup(myrdd2)

joinResult = myrdd1.join(myrdd2)
print(joinResult.collect())
print(result.collect()) 

print(" expand rdd from cogroup containing iterable items")
print(result.map(lambda l: (l[0], list(l[1][0]), list(l[1][1]))).collect())

# [
#  (2, (<pyspark.resultiterable.ResultIterable object at 0x048ADD70>, <pyspark.resultiterable.ResultIterable object at 0x048ADBD0>)), 
#  (4, (<pyspark.resultiterable.ResultIterable object at 0x048ADE10>, <pyspark.resultiterable.ResultIterable object at 0x048AD830>)), 
#  (6, (<pyspark.resultiterable.ResultIterable object at 0x048AD8B0>, <pyspark.resultiterable.ResultIterable object at 0x048ADC10>)), 
#  (1, (<pyspark.resultiterable.ResultIterable object at 0x048ADED0>, <pyspark.resultiterable.ResultIterable object at 0x048ADFB0>)), 
#  (3, (<pyspark.resultiterable.ResultIterable object at 0x048ADE70>, <pyspark.resultiterable.ResultIterable object at 0x048ADAF0>)), 
#  (5, (<pyspark.resultiterable.ResultIterable object at 0x048ADE50>, <pyspark.resultiterable.ResultIterable object at 0x048AD650>))
# ]

# list(l[1][0]) --> left
# list(l[1][1]) --> right


# product.cogroup(order.map((productKey, order{})))
# result -> rdd[( productId,  (  iterable(dict{product}), iterable(dict{order})  ) )]



# do either left or right join
# do the aggregation for join of product and orderdetails -> sum(quantity sold per product)

#total.foreach(lambda l: print(l))




