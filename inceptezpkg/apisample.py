import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, FloatType, MapType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("JsonSchemaExample for API Call") \
    .getOrCreate()

# Fetch data from API
url = "https://restcountries.com/v3.1/currency/usd"
response = requests.get(url)
json_data = response.json()
print(json_data)
# Define the schema
schema = StructType([
    StructField("name", StructType([
        StructField("common", StringType(), True),
        StructField("official", StringType(), True),
        StructField("nativeName", MapType(StringType(), StructType([
            StructField("official", StringType(), True),
            StructField("common", StringType(), True)
        ])), True)
    ]), True),
    StructField("tld", ArrayType(StringType()), True),
    StructField("cca2", StringType(), True),
    StructField("ccn3", StringType(), True),
    StructField("cca3", StringType(), True),
    StructField("cioc", StringType(), True),
    StructField("independent", BooleanType(), True),
    StructField("status", StringType(), True),
    StructField("unMember", BooleanType(), True),
    StructField("currencies", MapType(StringType(), StructType([
        StructField("name", StringType(), True),
        StructField("symbol", StringType(), True)
    ])), True),
    StructField("idd", StructType([
        StructField("root", StringType(), True),
        StructField("suffixes", ArrayType(StringType()), True)
    ]), True),
    StructField("capital", ArrayType(StringType()), True),
    StructField("altSpellings", ArrayType(StringType()), True),
    StructField("region", StringType(), True),
    StructField("subregion", StringType(), True),
    StructField("languages", MapType(StringType(), StringType()), True),
    StructField("translations", MapType(StringType(), StructType([
        StructField("official", StringType(), True),
        StructField("common", StringType(), True)
    ])), True),
    StructField("latlng", ArrayType(FloatType()), True),
    StructField("landlocked", BooleanType(), True),
    StructField("borders", ArrayType(StringType()), True),
    StructField("area", FloatType(), True),
    StructField("demonyms", MapType(StringType(), StructType([
        StructField("f", StringType(), True),
        StructField("m", StringType(), True)
    ])), True),
    StructField("flag", StringType(), True),
    StructField("maps", StructType([
        StructField("googleMaps", StringType(), True),
        StructField("openStreetMaps", StringType(), True)
    ]), True),
    StructField("population", IntegerType(), True),
    StructField("gini", MapType(StringType(), FloatType()), True),
    StructField("fifa", StringType(), True),
    StructField("car", StructType([
        StructField("signs", ArrayType(StringType()), True),
        StructField("side", StringType(), True)
    ]), True),
    StructField("timezones", ArrayType(StringType()), True),
    StructField("continents", ArrayType(StringType()), True),
    StructField("flags", StructType([
        StructField("png", StringType(), True),
        StructField("svg", StringType(), True),
        StructField("alt", StringType(), True)
    ]), True),
    StructField("coatOfArms", StructType([
        StructField("png", StringType(), True),
        StructField("svg", StringType(), True)
    ]), True),
    StructField("startOfWeek", StringType(), True),
    StructField("capitalInfo", StructType([
        StructField("latlng", ArrayType(FloatType()), True)
    ]), True)
])

spark.conf.set("spark.sql.debug.maxToStringFields", '1000')

# Create a DataFrame from the JSON data with the defined schema
df = spark.createDataFrame(json_data, schema=schema)

# Show the DataFrame schema
df.printSchema()

print(df.count())
# Show the DataFrame content
#df.show()

# Stop the Spark session
#spark.stop()
