from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 3")
sqlContext = SQLContext(sc)
# Read file from hadoop
temperature_file=sc.textFile("BDA/input/temperature-readings.csv")
# Split features of the file separated by a ';'
lines = temperature_file.map(lambda line: line.split(";"))
# Map key as year and value as temperature
tempReadingsRow = lines.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),
int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))

tempReadingsString = ["station", "date", "year", "month", "time", "value", "quality"]
# Apply the schema to the RDD.
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow, tempReadingsString)
# Register the DataFrame as a table.
schemaTempReadings.registerTempTable("tempReadingsTable")
filtered_years = schemaTempReadings.where('year >= 1960 and year <= 2014')
temp_day_max = filtered_years.groupBy(['year', 'month', 'station', 'date']).agg(F.max('value').alias('max'))
temp_day_min = filtered_years.groupBy(['year', 'month', 'station', 'date']).agg(F.min('value').alias('min'))
temp_day_maxmin = temp_day_max.join(temp_day_min, ['year', 'month', 'station', 'date'])
temp_day_maxmin = temp_day_maxmin.select('year', 'month', 'station', 'date', ((temp_day_maxmin.max+temp_day_maxmin.min)/2).alias('maxmin'))
temp_month_avg = temp_day_maxmin.groupBy(['year', 'month', 'station']).agg(F.avg('maxmin').alias('avgMonthlyTemperature'))
temp_month_avg.orderBy('avgMonthlyTemperature', ascending=0).rdd.saveAsTextFile("BDA/output/avg_temp_month_station")
