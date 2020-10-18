from pyspark import SparkContext
sc = SparkContext(appName = "exercise 5")
precipitation_file=sc.textFile("BDA/input/precipitation-readings.csv")
stations_file=sc.textFile("BDA/input/stations-Ostergotland.csv")
lines_precipitation = precipitation_file.map(lambda line: line.split(";"))
lines_stations = stations_file.map(lambda line: line.split(";"))
precipitation = lines_precipitation.map(lambda x: (x[0], x[1][0:4], x[1][5:7], float(x[3])))
precipitation = precipitation.filter(lambda x: int(x[1])>=1993 and int(x[1])<=2016)
# Converting RDD to a python list
stations = lines_stations.map(lambda x: (int(x[0]))).collect()
# Distribute list to all nodes since small file
stations_distributed = sc.broadcast(stations)
# Filtering all the stations in Östergötland
precipitation_ogotland = precipitation.filter(lambda a: int(a[0]) in stations_distributed.value)
# Mapping key to station, year and month and value as precipitation
precipitation_month = precipitation_ogotland.map(lambda x: ((x[0], x[1], x[2]), x[3]))
# Summing the total precipitation per key
prec_month_avg_station = precipitation_month.reduceByKey(lambda a,b: a+b)
# Mapping key to year and month and value to precipitation and a 1 for counting purposes
prec_month_avg = prec_month_avg_station.map(lambda x: ((x[0][1], x[0][2]), (x[1], 1)))
# Calculating the total precipitation and count per key
prec_month_avg = prec_month_avg.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
# Calculating average precipitation per key
prec_month_avg = prec_month_avg.mapValues(lambda x: x[0]/x[1])
prec_month_avg.saveAsTextFile("BDA/output/prec_month_avg_ogotland")
