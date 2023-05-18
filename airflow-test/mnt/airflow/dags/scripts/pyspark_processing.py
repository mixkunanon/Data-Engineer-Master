from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Forex processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file forex_rates.json from the HDFS
df = spark.read.csv('hdfs://namenode:9000/transaction/transaction_data.csv',header=True, inferSchema=True)
# Export the dataframe into the Hive table forex_rates
df.write.mode('overwrite').saveAsTable('default.transaction_table')
