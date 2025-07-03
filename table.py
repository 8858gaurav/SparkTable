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

    df3 = spark.read.csv("/Users/gauravmishra/Desktop/SparkTableCreation/Datasets/customers.csv", header=True, inferSchema=True)
    df3.createOrReplaceTempView("customers")

    spark.sql("SELECT * FROM customers").show(10)

    print("print table",spark.catalog.listTables())
    print("print db", spark.catalog.listDatabases())
    print('\n\n', spark.catalog.listColumns("customers"))

    # how to craete a same table under different spark session
    df3.createOrReplaceGlobalTempView("customers_global")

    spark.sql("SELECT * FROM global_temp.customers_global").show(10)
    print("global table details", spark.catalog.getTable("global_temp.customers_global"))
    print("print all table",spark.catalog.listTables())
    print("print all db", spark.catalog.listDatabases())
    print('\n\n', spark.catalog.listColumns("global_temp.customers_global"))

    # create a new session by using the same spark context
    spark2 = spark.newSession()
    spark2.sparkContext.setLogLevel('WARN')
    print('\n\n',"table in new session")
    spark2.sql("SELECT * FROM global_temp.customers_global").show(10)
