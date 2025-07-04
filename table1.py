# starting the history server
# sh /library/Frameworks/python.framework/versions/3.13/lib/python3.13/site-packages/pyspark/sbin/start-history-s
# erver.sh

# location of pyspark: /library/Frameworks/python.framework/versions/3.13/lib/python3.13/site-packages/pyspark/

import pyspark, time
from pyspark.sql.functions import * 
from pyspark.sql import SparkSession 
import getpass
username = getpass.getuser()
print(username)

if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
            .builder \
            .appName("debu application") \
            .config("spark.sql.shuffle.partitions", 3) \
            .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
            .config('spark.eventLog.enabled', 'true') \
            .config('spark.eventLog.dir', f"/Users/gauravmishra/Desktop/adding/SparkTable/tmp/spark-events") \
            .config('spark.history.fs.logDirectory', f"/Users/gauravmishra/Desktop/adding/SparkTable/tmp/spark-events") \
            .config('spark.history.ui.port', '18080') \
            .config('spark.history.fs.update.interval', '10s') \
            .config('spark.eventLog.compress', 'true') \
            .config('spark.ui.showConsoleProgress', 'true') \
            .enableHiveSupport() \
            .config("spark.driver.bindAddress","localhost") \
            .config("spark.ui.port","4041") \
            .master("local[*]") \
            .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    df = spark.read .option("header", "true").option("inferSchema", "true").option("dateFormat", "yyyy-MM-dd").csv("/Users/gauravmishra/Desktop/adding/SparkTable/Datasets/Lung_Cancer.csv", header=True, inferSchema=True)

    print("spark web UI URl", spark.sparkContext.uiWebUrl)

    print(df.columns)

    print(df.rdd.getNumPartitions())

    print(df.storageLevel)

    print(df.printSchema())

    # datatypes conversion
    df1 = df.select(
        col("age").cast("int"),
        col('id').cast("int"),
        col('gender').cast("string"),
        col('smoking_status').cast("string"),
        col('end_treatment_date').cast("date"))
    
#     df = df.withColumn('treatment_type', df.select(col('treatment_type')).cast('string')) 

    df1.printSchema()
    # will create a new table in the warehouse directory, will create the same no of files as the no of partitions
    # saveAsTextFile will work only with RDDs, not with DataFrames
    df.rdd.saveAsTextFile("/Users/gauravmishra/Desktop/adding/SparkTable-2/Output/lung_cancer")

    # to keep spark web UI alive locally
    time.sleep(86400000)

    
