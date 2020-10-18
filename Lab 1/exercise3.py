from pyspark import SparkContext
sc = SparkContext(appName = "exercise 3")
temperature_file=sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
year_temperature = lines.map(lambda x: (x[1][0:4], x[1][5:7], x[1][8:10], x[0], float(x[3])))
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1960 and int(x[0])<=2014)
temp_day = year_temperature.map(lambda x: ((int(x[0]), int(x[1]), int(x[2]), int(x[3])), x[4]))
# Finding the max and min temperature for each station for each day
temp_day_max = temp_day.reduceByKey(max)
temp_day_min = temp_day.reduceByKey(min)
# Creating a RDD with a value pair containing the max and min temperature for each station each day
temp_day_maxmin = temp_day_max.join(temp_day_min)
# Now mapping over year, month and station and averaging the max and min temperature and also adding a 1 to the value for counting purposes
temp_month = temp_day_maxmin.map(lambda x: ((x[0][0], x[0][1], x[0][3]), ((x[1][0]+x[1][1])/2, 1)))
# Summing all the measured daily averages and counting the number of days per month
temp_month_avg = temp_month.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
# Calculating the monthly averages
temp_month_avg_final = temp_month_avg.mapValues(lambda x: x[0]/x[1])
temp_month_avg_final.saveAsTextFile("BDA/output/avg_temp_month_station")
