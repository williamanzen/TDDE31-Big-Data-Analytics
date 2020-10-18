from pyspark import SparkContext
from math import radians, cos, sin, asin, sqrt, exp
from datetime import *
# import matplotlib.pyplot as plt

sc = SparkContext(appName="lab_kernel")
def haversine(lon1, lat1, lon2, lat2):
#Calculate the great circle distance between two points on the earth (specified in decimal degrees)
# convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
# haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km
h_distance = 100000
h_date = 20
h_time = 2
a = 59.4059 # Up to you
b = 18.0256 # Up to you
# Coordinates for Danderyd
date_value = "2013-07-04" # Up to you
# Converting to int to optimize filter function in later step
date_value_year=int(date_value[0:4])
date_value_month=int(date_value[5:7])
date_value_day=int(date_value[8:10])
stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temperature-readings.csv")

#Defining lists for plots
dist=list(range(1,100000))
date=list(range(-182,182))
time=list(range(-12,12))

# def plotDist(dist, h):
#     u=[number/h for number in dist]
#     u_final=[exp(-x**2) for x in u]
#     plt.figure(0)
#     plt.plot(dist, u_final)
#     plt.ylabel('Weight')
#     plt.xlabel('Distance')
#     plt.suptitle('Plot of kernel weights for distance')
#     plt.savefig("BDA/output/dist.png")
#     return None
#
# def plotDate(date, h):
#     u=[number/h for number in date]
#     u_final=[exp(-x**2) for x in u]
#     plt.figure(1)
#     plt.plot(date, u_final)
#     plt.ylabel('Weight')
#     plt.xlabel('Days')
#     plt.suptitle('Plot of kernel weights for days')
#     plt.savefig("BDA/output/date.png")
#     return None
#
# def plotTime(time, h):
#     u=[number/h for number in time]
#     u_final=[exp(-x**2) for x in u]
#     plt.figure(2)
#     plt.plot(time, u_final)
#     plt.ylabel('Weight')
#     plt.xlabel('Hours')
#     plt.suptitle('Plot of kernel weights for hours')
#     plt.savefig("BDA/output/time.png")
#     return None

# plotTime(time, h_time)
# plotDist(dist, h_distance)
# plotDate(date, h_date)

linesStations = stations.map(lambda line: line.split(";"))
stations = linesStations.map(lambda x: (x[0], (float(x[3]), float(x[4]))))
# Collecting stations list as a map and broadcasting this to all nodes since file is small in comparison to the temperatures file (yields better performance)
stations_distributed=sc.broadcast(stations.collectAsMap())
linesTemps = temps.map(lambda line: line.split(";"))
temps = linesTemps.map(lambda x: ((x[0], int(x[1][0:4]), int(x[1][5:7]), int(x[1][8:10]), int(x[2][0:2]), stations_distributed.value[x[0]][0], stations_distributed.value[x[0]][1]), float(x[3])))

# Function for filter the data and remove data at the days that are posterior to the reference date
def filterDate(dateYear, dateMonth, dateDay, data):
    compare_date = date(dateYear,dateMonth,dateDay)
    return(data.filter(lambda x: (compare_date>date(x[0][1],x[0][2],x[0][3]))))

# Function for filter the data and remove data at the hours that are posterior to the reference hour
def filterHour(time, data):
    return(data.filter(lambda x: (time>x[1])))

# Calculating kernel for difference in days
def gaussianDay(dateYear, dateMonth, dateDay, year, month, day, h):
    delta = (datetime(dateYear,dateMonth,dateDay)-datetime(year,month,day)).days % 365
    if(delta>=183):
        delta = 365 - delta
    u=delta/h
    return (exp(-u**2))

# Calculating kernel for difference in distance
def gaussianDist(placeA, placeB, data, h):
    lat=data[5]
    long=data[6]
    u=haversine(placeA, placeB, lat, long)/h
    return (exp(-u**2))

# Calculating kernel for difference in time
def gaussianTime(timeVal, timedata, h):
    delta = abs(timeVal-timedata)
    if(delta>=13):
        delta= 24 - delta
    u=delta/h
    return (exp(-u**2))

# Filter and remove the data points posterior to the reference day before the time loop to optimize performance
temps = filterDate(ourYear,ourMonth,ourDay,temps)
# Calculating kernels that do not depend of the hour of the day before the time loop to optimize performance
kernel = temps.map(lambda x: (x[1],x[0][4],
                               (gaussianDist(a, b, x[0], h_distance)+gaussianDay(ourYear,ourMonth,ourDay, x[0][1],x[0][2],x[0][3], h_date)),
                                ((gaussianDist(a, b, x[0], h_distance)+gaussianDay(ourYear,ourMonth,ourDay, x[0][1],x[0][2],x[0][3], h_date))*x[1]),
                                (gaussianDist(a, b, x[0], h_distance)*gaussianDay(ourYear,ourMonth,ourDay, x[0][1],x[0][2],x[0][3], h_date)),
                             (gaussianDist(a, b, x[0], h_distance)*gaussianDay(ourYear,ourMonth,ourDay, x[0][1],x[0][2],x[0][3], h_date)*x[1])))
# Saving this RDD to memory so it can be used in later stage
kernel.persist()

firstTime=True
# Looping through the different time points and storing the calculated predicted temperature through the specified kernel formulas
for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    intTime = int(time[0:2])
    kernelTemp = filterHour(intTime,kernel)
    kernelTemp = kernelTemp.map(lambda x: (1,((x[2]+gaussianTime(intTime, x[1], h_time)),
                                      (x[3]+(gaussianTime(intTime, x[1], h_time)*x[0])),
                                     (x[4]*gaussianTime(intTime, x[1], h_time)),
                                      (x[5]*(gaussianTime(intTime, x[1], h_time))))))
    kernelTemp = kernelTemp.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1],a[2]+b[2],a[3]+b[3]))
    kernelTemp = kernelTemp.mapValues(lambda a: (a[1]/a[0], a[3]/a[2]))
    if firstTime:
        kernelsum = kernelTemp.map(lambda x: (time, x[1][0]))
        kernelmult = kernelTemp.map(lambda x: (time, x[1][1]))
        firstTime = False
    else:
        kernelsum = kernelsum.union(kernelTemp.map(lambda x: (time, x[1][0])))
        kernelmult = kernelmult.union(kernelTemp.map(lambda x: (time, x[1][1])))

# Shrinking the output to only one file and saving it to output folder
kernelsum.coalesce(1).saveAsTextFile("BDA/output/sum")
kernelmult.coalesce(1).saveAsTextFile("BDA/output/mult")
# plt.figure(3)
# plt.plot(time_vector, kernel_sum)
# plt.xlabel('Time of day')
# plt.ylabel('Temperature')
# plt.suptitle('Temperature estimate through sum of factors')
# plt.savefig("BDA/output/sum.png")
# plt.figure(4)
# plt.plot(time_vector, kernel_mult)
# plt.xlabel('Time of day')
# plt.ylabel('Temperature')
# plt.suptitle('Temperature estimate through product of factors')
# plt.savefig("BDA/output/mult.png")
