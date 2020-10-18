from pyspark import SparkContext
sc = SparkContext(appName = "exercise 4")
temperature_file=sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file=sc.textFile("BDA/input/precipitation-readings.csv")
lines_temperature = temperature_file.map(lambda line: line.split(";"))
lines_precipitation = precipitation_file.map(lambda line: line.split(";"))
temperature = lines_temperature.map(lambda x: (x[0], float(x[3])))
precipitation = lines_precipitation.map(lambda x: (x[0], float(x[3])))
# Finding the max temp and max precipitation for each station
temp_max = temperature.reduceByKey(max)
prec_max = precipitation.reduceByKey(max)
# Creating a RDD with a value pair containing the max temperature and max precipitation for each station
joined_max = temp_max.join(prec_max)
# FIltering all instances where the temperature and precipitation is within a specified range
filtered_data = joined_max.filter(lambda x: float(x[1][0])>=25 and float(x[1][0])<=30 and float(x[1][1])>=100 and float(x[1][1])<=200)
filtered_data.saveAsTextFile("BDA/output/max_temp_prec")
