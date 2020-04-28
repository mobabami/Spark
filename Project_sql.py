from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSql").getOrCreate()
sc = spark.sparkContext

# ---------------------------------What to do 
dataRDD = sc.parallelize([("Apple", 20), ("Orange", 25), ("Orange", 80), ("Peach", 40), ("Berry", 15)])

avgRDD = dataRDD.map(lambda l: (l[0], (l[1], 1))) \
    .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]) ) \
    .map(lambda l: (l[0], l[1][0] / l[1][1]))

print(avgRDD.collect())

# ---------------------------------What you wish to do -------- expressive approach

from pyspark.sql.functions import avg

fruit_df = spark.createDataFrame( \
    [("Apple", 20), ("Orange", 25), ("Orange", 80), ("Peach", 40), ("Berry", 15)],  \
    ["name", "quantity"] \
)

avg_df = fruit_df.groupBy("name").agg(avg("quantity"))

avg_df.show()
