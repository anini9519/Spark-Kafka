from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json,col,sum,window,to_json,struct
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType
spark   = SparkSession.builder.appName("kafka_project").getOrCreate()

df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "order1")\
    .option("includeTimestamp","true")\
    .load()

spark.sparkContext.setLogLevel("WARN")

dfDeserialized = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","CAST(timestamp AS STRING)")

schema = StructType(
            [
                StructField("order_id", IntegerType()),
                StructField("item_id", StringType()),
                StructField("qty", IntegerType()),
                StructField("price", IntegerType()),
                StructField("state", StringType()),
            ]
)

jsonDf = dfDeserialized.withColumn("value", from_json("value", schema))\
    .select(col('value.*'),col("timestamp"))

Amount_Df= jsonDf.withColumn("Amount",col('qty')*col('price')).groupBy(window(col("timestamp"),"5 Seconds"),col('state'))\
    .agg(sum('Amount').alias('final_amount'))\
        .select(col('state'),col('final_amount'))

#resultDf = amount_Df.writeStream.format("console").outputMode("complete").start().awaitTermination()


# json_conversion = converted_col.withColumn
json_conversion = Amount_Df.withColumn("value",to_json(struct([Amount_Df[x] for x in Amount_Df.columns])))

# json_conversion.writeStream.format("console").outputMode("complete").start().awaitTermination()                                

converted_col = json_conversion.withColumnRenamed("state","key")\
                .select(col("key"),col("value"))


converted_col.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka")\
    .outputMode("complete")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "statewiseearning")\
    .option("checkpointLocation", "/home/ubuntu/checkpoint1") \
    .start() \
        

def toDB(microBatchDf, id):
    microBatchDf \
        .write \
        .format('jdbc') \
        .option("url", "jdbc:postgresql://bigdata.cf8emgih4asw.us-east-2.rds.amazonaws.com:5432/ak_productdb") \
        .option("dbtable", "statewiseearning") \
        .option("user", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("password", "Root12$34") \
        .mode("append")\
        .save()





Amount_Df.writeStream \
    .foreachBatch(lambda batch,id : toDB(batch,id)) \
    .outputMode("update") \
    .start() \
   .awaitTermination()         
                          