from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 4")
sqlContext = SQLContext(sc)
# Read file from hadoop
temperature_file=sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file=sc.textFile("BDA/input/precipitation-readings.csv")
# Split features of the file separated by a ';'
lines_temperature = temperature_file.map(lambda line: line.split(";"))
lines_precipitation = precipitation_file.map(lambda line: line.split(";"))
# Map key as year and value as temperature
tempReadingsRow = lines_temperature.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),
int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
precReadingsRow = lines_precipitation.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),
int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))

tempReadingsString = ["station", "date", "year", "month", "time", "temp", "quality"]
precReadingsString = ["station", "date", "year", "month", "time", "prec", "quality"]
# Apply the schema to the RDD.
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow, tempReadingsString)
schemaPrecReadings = sqlContext.createDataFrame(precReadingsRow, precReadingsString)
# Register the DataFrame as a table.
schemaTempReadings.registerTempTable("tempReadingsTable")
schemaPrecReadings.registerTempTable("precReadingsTable")
temp_max = schemaTempReadings.groupBy('station').agg(F.max('temp').alias('maxtemp'))
prec_max = schemaPrecReadings.groupBy('station').agg(F.max('prec').alias('maxprec'))
joined_max = temp_max.join(prec_max, 'station').where('maxtemp >= 25 and maxtemp <= 30 and maxprec >= 100 and maxprec <= 200')
joined_max.orderBy('station', ascending=0).rdd.saveAsTextFile("BDA/output/max_temp_prec")
