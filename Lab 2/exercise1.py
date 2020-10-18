from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 1")
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
# Filter the table
filtered_years = schemaTempReadings.where('year >= 1950 and year <= 2014')
# Find max temp per year
max_temp = filtered_years.groupBy('year').agg(F.max('value').alias('value'))
# Find min temp per year
min_temp = filtered_years.groupBy('year').agg(F.min('value').alias('value'))
# For all max and min values rejoin table with original one to obtain column station
max_temp = max_temp.join(filtered_years, ['year', 'value']).select('year', 'station', 'value').orderBy(['value'], ascending=[0])
min_temp = min_temp.join(filtered_years, ['year', 'value']).select('year', 'station', 'value').orderBy(['value'], ascending=[0])
# Renaming column 'value' and saving result to output
max_temp.withColumnRenamed('value', 'yearlyMax').rdd.saveAsTextFile("BDA/output/max_temperature")
min_temp.withColumnRenamed('value', 'yearlyMin').rdd.saveAsTextFile("BDA/output/min_temperature")
