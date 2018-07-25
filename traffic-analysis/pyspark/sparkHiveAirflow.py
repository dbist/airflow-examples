from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('hdfs://aervits-hdp0:8020/apps/hive')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# spark.sql("SELECT COUNT(DT), TM, BOROUGH FROM TRAFFIC GROUP BY TM, BOROUGH").show()
sqlDf = spark.sql("SELECT COUNT(DT), TM, BOROUGH FROM TRAFFIC GROUP BY TM, BOROUGH")
spark.sql("DROP TABLE IF EXISTS trafficDf")
sqlDf.createOrReplaceTempView("trafficDf")
spark.sql("CREATE TABLE trafficDF stored as ORC AS SELECT * FROM trafficDf")
