from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("SimpleApp").getOrCreate()

counter = 0
rdd = sc.parallelize([1,2,3,4])

# Wrong: Don't do this!!
def increment_counter(x):
    global counter
    counter += x
    
rdd.foreach(increment_counter)

print("Counter value: ", counter)

## Printing elements of an RDD !!!! same mistake!