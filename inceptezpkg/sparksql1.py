from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[1]").appName("Spark core pyspark").getOrCreate()
   #Set the logger level to error
spark.sparkContext.setLogLevel("ERROR")
flight_csv = spark.read.format("csv").option("header", "true").option("inferSchema", "true").\
    load("E:/INCEPTEZ/TRAINING MATERIALS IZ RAW BOOKS/13 SPARK/PYSPARK/PYSPARK_2022/flight_summary.csv")
flight_csv.createOrReplaceTempView("flight_csv_tbl")

# count
flight_csv.count()

# select operation
flight_csv.select("DEST_COUNTRY_NAME", col("DEST_COUNTRY_NAME"), flight_csv[0], flight_csv["DEST_COUNTRY_NAME"]).show()

flight_csv.select("*").show()

# literals/constants
flight_csv.select(expr("*"), lit("dummy").alias("constant_column")).show()

# Adding columns
flight_csv = flight_csv.withColumn("constant_col1", lit("dummy1"))
flight_csv = flight_csv.withColumn("constant_col2", lit("dummy2"))

flight_csv.columns
flight_csv.select(expr("constant_col2 + 5")).show()


# Renaming column
flight_csv.withColumnRenamed("DEST_COUNTRY_NAME", "dest_country").printSchema()
flight_csv.withColumnRenamed("ORIGIN_COUNTRY_NAME", "src_country").printSchema()

# dropping column
flight_csv.drop("constant_col1").printSchema()

# distinct count
flight_csv.select("ORIGIN_COUNTRY_NAME").distinct().rdd.getNumPartitions()
flight_csv.select("ORIGIN_COUNTRY_NAME").distinct().count()

# filter
# SyntaxError: keyword can't be an expression if == is  not used.
to_usa = flight_csv.where(col("DEST_COUNTRY_NAME") == "United States").count()
from_usa = flight_csv.filter("ORIGIN_COUNTRY_NAME = 'United States'").count()

# ordering
flight_csv.sort("count").show()


# limit
flight_csv.limit(10).show()

# casting
flight_csv.withColumn("count_string", col("count").cast("string")).printSchema()

# numeric operations
retail_csv = spark.read.format("csv").\
    option("header", "true").option("inferSchema", "true").load("E:/INCEPTEZ/TRAINING MATERIALS IZ RAW BOOKS/13 SPARK/PYSPARK/PYSPARK_2022/data/retail_per_day.csv")

total_price = (col("Quantity") * col("UnitPrice"))
retail_csv.select(expr("CustomerId"), total_price.alias("total")).show()

retail_csv_new = retail_csv.withColumn("total", total_price)

retail_csv_new.groupBy(col('CustomerId')).agg(round(mean(col('total'))).alias('avg_expense')).show()

# string operations

retail_csv.select(col("Description"),
                  lower(col("Description")), upper(lower(col("Description"))), initcap(col("Description"))).show()

# null operations
retail_csv.count()

retail_csv.na.drop("any").count()
retail_csv.na.drop("all").count()

retail_csv.na.fill("NA").count()
fillers = {"StockCode": 1000, "Description" : "NA"}
retail_csv.na.fill(fillers)
# retrieve filled records
retail_csv.na.fill(fillers).filter(col("Description") == "NA").show()

retail_csv_all = spark.read.format("csv").\
    option("header", "true").option("inferSchema", "true").load("E:/INCEPTEZ/TRAINING MATERIALS IZ RAW BOOKS/13 SPARK/PYSPARK/PYSPARK_2022/data/retail_all_day.csv")
retail_csv_all.cache()

retail_csv_all.count()

# count
retail_csv_all.select(count("StockCode")).show()

# first & last
retail_csv_all.select(first("StockCode"), last("StockCode")).show()

# min and max
retail_csv_all.select(min("Quantity"), max("Quantity")).show()

# sum
retail_csv_all.select(sum("Quantity")).show()

# converting existing date format in data to standard date field
# this will work only above spark 2.2
retail_csv_updated = retail_csv_all.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))

# for spark prior to 2.2 as to_date will take only one argument
# retail_csv_updated = retail_csv_all.withColumn("date", to_date(unix_timestamp(col("InvoiceDate"), "MM/d/yyyy H:mm").cast("timestamp")))


# windowing functions
from pyspark.sql.window import Window

window_spec = Window.partitionBy("CustomerId", "date").orderBy(desc("Quantity"))
purchase_dense_rank = dense_rank().over(window_spec)
purchase_rank = rank().over(window_spec)
purchase_row = row_number().over(window_spec)

retail_csv_updated.where("CustomerId IS NOT NULL").\
    orderBy("CustomerId").\
    select(col("CustomerId"), col("date"), col("Quantity"), purchase_rank.alias("quantityRank"),
           purchase_dense_rank.alias("quantityDenseRank")).show()


retail_csv_updated.where("CustomerId IS NOT NULL").orderBy("CustomerId").\
    select(col("CustomerId"), col("date"), col("Quantity"), purchase_rank.alias("quantityRank"),
           purchase_dense_rank.alias("quantityDenseRank"), purchase_row.alias("QuantityRowNum")).show()

# joins

orders = spark.read.csv('E:/INCEPTEZ/TRAINING MATERIALS IZ RAW BOOKS/13 SPARK/PYSPARK/PYSPARK_2022/data/orders.csv',header=True,inferSchema=True)
order_items = spark.read.csv('E:/INCEPTEZ/TRAINING MATERIALS IZ RAW BOOKS/13 SPARK/PYSPARK/PYSPARK_2022/data/order_items.csv',header=True,inferSchema=True)
customers = spark.read.csv('E:/INCEPTEZ/TRAINING MATERIALS IZ RAW BOOKS/13 SPARK/PYSPARK/PYSPARK_2022/data/customers.csv',header=True,inferSchema=True)

inner_res1 = orders.join(order_items, orders.order_id == order_items.order_item_order_id)
inner_res1.show()
inner_res2 = orders.join(order_items, (orders.order_id == order_items.order_item_order_id) &
                         (orders.order_id == order_items.order_item_order_id))
inner_res2.show()
customers.join(orders, customers.customer_id==orders.order_customer_id, 'left').show()
customers.join(orders, customers.customer_id==orders.order_customer_id, 'right').show()
print(customers.join(orders, customers.customer_id==orders.order_customer_id, 'left').count())
print(customers.join(orders, customers.customer_id==orders.order_customer_id, 'right').count())
