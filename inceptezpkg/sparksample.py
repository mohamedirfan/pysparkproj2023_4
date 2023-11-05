from pyspark.sql.session import SparkSession
print("i am a python program")
#python program
lst=[10,20,30,40]
for i in lst:
    print(i+5)
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
#spark program to perform large scale data processing on the cluster in a paralle and distributed fashion
# leveraging the power of cheap memory
spark=SparkSession.builder.getOrCreate()
sc=spark.sparkContext
rdd1=sc.parallelize(lst)
rdd2=rdd1.map(lambda x:x+5)
rdd2.collect()
print(rdd2.count())