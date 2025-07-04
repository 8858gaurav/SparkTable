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
            .enableHiveSupport() \
            .config("spark.driver.bindAddress","localhost") \
            .config("spark.ui.port","4040") \
            .master("local[*]") \
            .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    df = spark.read.csv("/Users/gauravmishra/Desktop/adding/SparkTable/Datasets/Lung_Cancer.csv", header=True, inferSchema=True)

    print(df.columns)

    print(df.rdd.getNumPartitions())

    print(df.storageLevel)
    
    # to keep spark web UI alive
    time.sleep(86400000)
