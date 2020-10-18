from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)
# Read file from hadoop
temperature_file=sc.textFile("BDA/input/temperature-readings.csv")
# Split features of the file separated by a ';'
lines = temperature_file.map(lambda line: line.split(";"))
# Map key as year and value as temperature
tempReadingsRow = lines.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),
int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))

tempReadingsString = ["station", "date", "year", "month", "time", "value",
"quality"]
# Apply the schema to the RDD.
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow,
tempReadingsString)
# Register the DataFrame as a table.
schemaTempReadings.registerTempTable("tempReadingsTable")
filtered_years = schemaTempReadings.where('year >= 1950 and year <= 2014 and value > 10')
month_over_10 = filtered_years.groupBy(['year', 'month']).agg(F.count('value').alias('value'))
month_over_10_distinct = filtered_years.groupBy(['year', 'month', 'station']).agg(F.countDistinct('year', 'month', 'station').alias('value'))
month_over_10_distinct = month_over_10_distinct.groupBy(['year', 'month']).agg(F.count('value').alias('value'))
sorted_over_10 = month_over_10.orderBy('value', ascending=0).rdd.saveAsTextFile("BDA/output/month_count")
sorted_distinct_over_10 = month_over_10_distinct.orderBy('value', ascending=0).rdd.saveAsTextFile("BDA/output/month_count_distinct")
