import pyspark
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
            .enableHiveSupport() \
            .config("spark.driver.bindAddress","localhost") \
            .config("spark.ui.port","4040") \
            .master("local[*]") \
            .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    print("yes")
    print("db", spark.sql("show databases").collect(), spark.catalog.listDatabases())
    # db [Row(namespace='default')] [Database(name='default', catalog='spark_catalog', description='Default Hive database', locationUri='file:/user/gauravmishra/warehouse')]
    print("table", spark.sql("show tables").collect(), spark.catalog.listTables())

    orders_df = spark.read.csv("/Users/gauravmishra/Desktop/Adding/SparkTable/orders_wh.csv", header = True, inferSchema= True)
    orders_df.show(5)

    orders_df = orders_df.withColumnRenamed("order_status", "status")
    orders_df = orders_df.withColumn("order_date", orders_df["order_date"].cast("date"))

    orders_df.printSchema()

    print(orders_df.columns)

    # df to table creation, it's a spark table, it's a distributed across the cluster.
    # create a table orders_itv016380 in the default database
    orders_df.createOrReplaceTempView("orders_itv016380")

    # table to df 
    orders_df_new = spark.read.table("orders_itv016380")
    orders_df_new.show(5)

    # create a db under /user/{username}/warehouse/
    spark.sql("create database if not exists itv016380_vs")

    spark.sql('show tables').filter("tableName = 'orders_itv016380'")

    spark.sql("show databases").filter("namespace = 'itv016380_vs'")
    # this db will be created under here: /user/{username}/warehouse"
    # !hadoop fs -ls /user/itv016380/warehouse/, then !hadoop fs -ls /user/itv016380/warehouse/itv016380_vs.db/

    spark.sql("create table itv016380_vs.orders (order_id integer, order_date date, customer_id integer, order_status string)")

    # instering the data from a temp view.
    spark.sql("insert into itv016380_vs.orders (select * from orders_itv016380)")
    spark.sql("describe table itv016380_vs.orders")
    spark.sql("describe extended itv016380_vs.orders").show(truncate = False)
    
# +----------------------------+-----------------------------------------------------------------------------+-------+
# |col_name                    |data_type                                                                    |comment|
# +----------------------------+-----------------------------------------------------------------------------+-------+
# |order_id                    |int                                                                          |null   |
# |order_date                  |date                                                                         |null   |
# |customer_id                 |int                                                                          |null   |
# |order_status                |string                                                                       |null   |
# |                            |                                                                             |       |
# |# Detailed Table Information|                                                                             |       |
# |Database                    |itv016380_vs                                                                 |       |
# |Table                       |orders                                                                       |       |
# |Owner                       |itv016380                                                                    |       |
# |Created Time                |Tue Jul 08 08:27:25 EDT 2025                                                 |       |
# |Last Access                 |UNKNOWN                                                                      |       |
# |Created By                  |Spark 3.1.2                                                                  |       |
# |Type                        |MANAGED                                                                      |       |
# |Provider                    |hive                                                                         |       |
# |Table Properties            |[transient_lastDdlTime=1751977736]                                           |       |
# |Statistics                  |840836625 bytes                                                              |       |
# |Location                    |hdfs://m01.itversity.com:9000/user/itv016380/warehouse/itv016380_vs.db/orders|       |
# |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                           |       |
# |InputFormat                 |org.apache.hadoop.mapred.TextInputFormat                                     |       |
# |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat                   |       |
# +----------------------------+-----------------------------------------------------------------------------+-------+


    #!hadoop fs -ls -h /user/itv016380/warehouse/itv016380_vs.db/orders, you will find 9 files under this folder, since we have 9 partitions in hdfs.
    # this is a managed table, so the data will be deleted when the table is dropped. this table is stored in the warehouse directory.
    # this table will be available in the hive metastore, so you can query it from hive as well, and it's managed by hive.

    # gives hadoop file locations only, then it will create an external tables.
    # inside this folder, we've a file called : /user/itv016380/data_vs/orders_wh.csv
    # this is our hadoop file location, so we can create an external table on top of this file.
    spark.sql("create table if not exists itv016380_vs.orders_ext (order_id integer, order_date string, customer_id integer, order_status string) using csv location '/user/itv016380/data_vs/")

    # a new csv file will be createded under this folder: /user/itv016380/data_vs/, when we called the insert command.
    #  e.g spark.sql("insert into itv016380_vs.orders_ext values(1111, '12-02-2023', 222, 'CLOSEED')")

    spark.sql("describe extended itv016380_vs.orders_ext").show(truncate = False)

#  +----------------------------+---------------------------------------------------------+-------+
# |col_name                    |data_type                                                |comment|
# +----------------------------+---------------------------------------------------------+-------+
# |order_id                    |int                                                      |null   |
# |order_date                  |string                                                   |null   |
# |customer_id                 |int                                                      |null   |
# |order_status                |string                                                   |null   |
# |                            |                                                         |       |
# |# Detailed Table Information|                                                         |       |
# |Database                    |itv016380_vs                                             |       |
# |Table                       |orders_ext                                               |       |
# |Owner                       |itv016380                                                |       |
# |Created Time                |Tue Jul 08 09:26:11 EDT 2025                             |       |
# |Last Access                 |UNKNOWN                                                  |       |
# |Created By                  |Spark 3.1.2                                              |       |
# |Type                        |EXTERNAL                                                 |       |
# |Provider                    |csv                                                      |       |
# |Location                    |hdfs://m01.itversity.com:9000/user/itv016380/data_vs     |       |
# |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       |       |
# |InputFormat                 |org.apache.hadoop.mapred.SequenceFileInputFormat         |       |
# |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat|       |
# +----------------------------+---------------------------------------------------------+-------+