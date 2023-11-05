from pyspark.sql.types import *
schema = StructType([
        StructField("results", ArrayType(StructType([
                     StructField("gender", StringType()),
                     StructField("location",
                             StructType([StructField("city", StringType()),StructField("state", StringType()),StructField("country", StringType()),StructField("postcode", StringType()),
                             StructField("coordinates", StructType([StructField("latitude", StringType()),StructField("longitude", StringType())])),])),
                     StructField("name", StructType([StructField("title", StringType()),StructField("first", StringType()),StructField("last", StringType())])),
                     StructField("email", StringType()),
                     StructField("login", StructType([StructField("uuid", StringType()),StructField("username", StringType())])),
                     StructField("dob", StructType([StructField("date", StringType()),StructField("age", StringType())])),
                     StructField("registered", StructType([StructField("date", StringType()),StructField("age", StringType())])),

        ])))])

json1='{"results":[{"gender":"male","name":{"title":"Mr","first":"Darrell","last":"Price"},"location":{"street":{"number":9806,"name":"Church Lane"},"city":"Newcastle West","state":"Wexford","country":"Ireland","postcode":25319,"coordinates":{"latitude":"-1.6482","longitude":"-7.5761"},"timezone":{"offset":"+10:00","description":"Eastern Australia, Guam, Vladivostok"}},"email":"darrell.price@example.com","login":{"uuid":"3bff1bf2-2cd4-49eb-8014-751cc69f3c2e","username":"beautifulfish500","password":"1226","salt":"kX1qJ5UY","md5":"968fc9ac86e06a543861bd2e484e89b7","sha1":"eed9bdfd2c3ff88180f9c412417ba967da066678","sha256":"a97d77cd0b7c71435471c59940aa92976873beded59c2277a196e2285e023acc"},"dob":{"date":"1965-08-22T03:04:09.753Z","age":57},"registered":{"date":"2003-06-28T02:26:19.004Z","age":20},"phone":"061-359-2719","cell":"081-907-1068","id":{"name":"PPS","value":"3547001T"},"picture":{"large":"https://randomuser.me/api/portraits/men/77.jpg","medium":"https://randomuser.me/api/portraits/med/men/77.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/men/77.jpg"},"nat":"IE"}],"info":{"seed":"2bec16e97157bc6b","results":1,"page":1,"version":"1.4"}}'
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.enableHiveSupport().getOrCreate()
jsondf=spark.read.schema(schema).json("E:/apidata.json",multiLine=True)
jsondf.printSchema()
jsondf.show(10,False)
jsondf.createOrReplaceTempView("usdataview")

exploded_df = jsondf.selectExpr('explode(results) as exploded_cols')

    # exploded_df.printSchema()

flattened_df = exploded_df \
        .withColumn('gender', col('exploded_cols.gender')) \
        .withColumn('city',col('exploded_cols.location.city'))\
        .withColumn('state',col('exploded_cols.location.state'))\
        .withColumn('title', col('exploded_cols.name.title')) \
        .withColumn('first',col('exploded_cols.name.first'))\
        .withColumn('last',col('exploded_cols.name.last'))\
        .withColumn('latitude',col('exploded_cols.location.coordinates.latitude'))\
        .withColumn('longitude',col('exploded_cols.location.coordinates.longitude'))\
        .withColumn('email', col('exploded_cols.email')) \
        .withColumn('uuid',col('exploded_cols.login.uuid'))\
        .withColumn('username',col('exploded_cols.login.username'))\
        .withColumn('dob_dt',col('exploded_cols.dob.date'))\
        .withColumn('dob_age',col('exploded_cols.dob.age'))\
        .withColumn('login_dt', col('exploded_cols.registered.date')) \
        .withColumn('login_age',col('exploded_cols.registered.age'))\
        .drop('exploded_cols')

flattened_df.show()

flattened_df.createOrReplaceTempView("jsonview")

#flattened_df.write.saveAsTable("default.jsondata_table",mode="overwrite")
#spark.read.table("default.jsondata_table").show()