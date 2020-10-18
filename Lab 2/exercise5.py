from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 5")
sqlContext = SQLContext(sc)
# Read file from hadoop
precipitation_file=sc.textFile("BDA/input/precipitation-readings.csv")
stations_file=sc.textFile("BDA/input/stations-Ostergotland.csv")
lines_precipitation = precipitation_file.map(lambda line: line.split(";"))
lines_stations = stations_file.map(lambda line: line.split(";"))
precReadingsRow = lines_precipitation.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),
int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
stationReadingsRow = lines_stations.map(lambda x: (x[0], x[1]))

stationReadingsString = ["station", "name"]
precReadingsString = ["station", "date", "year", "month", "time", "prec", "quality"]
# Apply the schema to the RDD.
schemaStationReadings = sqlContext.createDataFrame(stationReadingsRow, stationReadingsString)
schemaPrecReadings = sqlContext.createDataFrame(precReadingsRow, precReadingsString)
# Register the DataFrame as a table.
schemaStationReadings.registerTempTable("stationReadingsTable")
schemaPrecReadings.registerTempTable("precReadingsTable")
# Filtering all the stations in OstergOtland
prec_ogotland = schemaPrecReadings.join(schemaStationReadings, 'station')
prec_month = prec_ogotland.groupBy(['station', 'year', 'month']).agg(F.sum('prec').alias('prec'))
prec_month_avg = prec_month.groupBy(['year', 'month']).agg(F.avg('prec').alias('avgMonthlyPrec'))
prec_month_avg.orderBy(['year', 'month'], ascending=[0,0]).rdd.saveAsTextFile("BDA/output/prec_month_avg_ogotland")
