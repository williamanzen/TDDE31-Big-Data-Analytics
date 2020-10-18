from pyspark import SparkContext
# Definition for calculating the maximum value
def max_temperature(a,b):
    if a>=b:
        return a
    else:
        return b
# Definition for calculating the minimum value
def min_temperature(a,b):
    if a<=b:
        return a
    else:
        return b
sc = SparkContext(appName = "exercise 1")
# Read file from hadoop
temperature_file=sc.textFile("BDA/input/temperature-readings.csv")
# Split features of the file separated by a ';'
lines = temperature_file.map(lambda line: line.split(";"))
# Map key as year and value as temperature
year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3])))
# Filter relevant data
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)
# Find max and min temperature for each key (year)
max_temperatures = year_temperature.reduceByKey(max_temperature)
min_temperatures = year_temperature.reduceByKey(min_temperature)
# Sort the result by descending temperature
max_temperaturesSorted = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])
min_temperaturesSorted = min_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])
# Save the file to a specified folder at hadoop
max_temperaturesSorted.saveAsTextFile("BDA/output/max_temperature")
min_temperaturesSorted.saveAsTextFile("BDA/output/min_temperature")
