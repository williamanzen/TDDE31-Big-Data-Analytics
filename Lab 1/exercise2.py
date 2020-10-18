from pyspark import SparkContext
sc = SparkContext(appName = "exercise 2")
temperature_file=sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
year_temperature = lines.map(lambda x: (x[0], x[1][0:4], x[1][5:7], float(x[3])))
year_temperature = year_temperature.filter(lambda x: int(x[1])>=1950 and int(x[1])<=2014 and float(x[3])>10)
# Add a 1 to the value of each data point which have a temperature above 10
month_over_10 = year_temperature.map(lambda x: (( int(x[1]) , int(x[2])), 1))
# Only count 1 value for each station once per month with temp above 10
month_over_10_distinct = year_temperature.map(lambda x: ((int(x[0]), int(x[1]), int(x[2])), 1)).distinct()
# Add a 1 to the value of each data point which have a temperature above 10 (distinct)
month_over_10_distinct = month_over_10_distinct.map(lambda x: ((x[0][1], x[0][2]), 1))
# Sum over all 1:s to count all instances
count_month_over_10 = month_over_10.reduceByKey(lambda a,b: a+b)
count_month_over_10_distinct = month_over_10_distinct.reduceByKey(lambda a,b: a+b)
count_month_over_10.saveAsTextFile("BDA/output/month_count")
count_month_over_10_distinct.saveAsTextFile("BDA/output/month_count_distinct")
