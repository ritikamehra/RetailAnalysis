# Import Dependencies 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


# Utility method for checking total cost for each invoice   
def util_total_cost(items,type):
   total_price = 0
   for item in items:
       total_price = total_price + item['unit_price'] * item['quantity']
   if type=="RETURN":
       return total_price * (-1)
   else:
       return total_price

# Utility method for checking total items count for each invoice   
def util_total_item_count(items):
   return len(items)

# Utility method for checking order type for each invoice
def util_is_order(column):
    if column == "ORDER":
        return 1
    else:
        return 0

# Utility method for checking return type for each invoice
def util_is_return(column):
    if column == "RETURN":
        return 1
    else:
        return 0

#Create spark session
spark = SparkSession  \
        .builder  \
        .appName("RetailDataAnalysis")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Read Input Stream from Kafka
sourceStream = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
        .option("subscribe","real-time-project")  \
        .load()

# Define Schema
inputSchema = StructType() \
        .add("invoice_no", LongType()) \
	    .add("country",StringType()) \
        .add("timestamp", TimestampType()) \
        .add("type", StringType()) \
        .add("total_items",IntegerType())\
        .add("is_order",IntegerType()) \
        .add("is_return",IntegerType()) \
        .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType()) 
        ])))


#Converting value to string to access the data
orderStream = sourceStream.select(from_json(col("value").cast("string"), inputSchema).alias("data")).select("data.*")


# Define the UDFs with the utility functions
total_cost = udf(util_total_cost, FloatType())
total_item_count = udf(util_total_item_count, IntegerType())
is_order = udf(util_is_order, IntegerType())
is_return = udf(util_is_return, IntegerType())

# Preparing final input stream by adding columns calculated using UDFs
orderInputStream = orderStream \
       .withColumn("total_items", total_item_count(orderStream.items)) \
       .withColumn("total_cost", total_cost(orderStream.items,orderStream.type)) \
       .withColumn("is_order", is_order(orderStream.type)) \
       .withColumn("is_return", is_return(orderStream.type))

#Writing final input stream to console by selecting required columns
order_query_console = orderInputStream \
       .select("invoice_no", "country", "timestamp","total_cost","total_items","is_order","is_return") \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .option("checkpointLocation", "checkpoint/") \
       .start()

# Calculate time based KPIs
aggTimeKPI = orderInputStream \
    .withWatermark("timestamp","1 minute") \
    .groupby(window("timestamp", "1 minute","1 minute")) \
    .agg(format_number(sum("total_cost"),2).alias("total_sales_volume"),
        format_number(avg("total_cost"),2).alias("average_transaction_size"),
        format_number(avg("is_Return"),2).alias("rate_of_return")) \
    .select("window.start","window.end","total_sales_volume","average_transaction_size","rate_of_return")

# Calculate time and country based KPIs
aggTimeCountryKPI = orderInputStream \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute", "1 minute"), "country") \
    .agg(format_number(sum("total_cost"),2).alias("total_sales_volume"),
        count("invoice_no").alias("OPM"),
        format_number(avg("is_Return"),2).alias("rate_of_return")) \
    .select("window.start","window.end","country", "OPM","total_sales_volume","rate_of_return")


# Write time based KPI values as json file
queryTimeKPI = aggTimeKPI.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "time_kpi/") \
    .option("checkpointLocation", "checkpoint/time_kpi/") \
    .trigger(processingTime="1 minutes") \
    .start()


# Write time and country based KPI values as json file
queryTimeCountryKPI = aggTimeCountryKPI.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "country_kpi/") \
    .option("checkpointLocation", "checkpoint/country_kpi/") \
    .trigger(processingTime="1 minutes") \
    .start()

queryTimeCountryKPI.awaitTermination()