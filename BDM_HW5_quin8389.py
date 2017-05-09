from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import HiveContext

citi = 'citibike.csv'
yellow = 'yellow.csv.gz'

def getRides(mapId, pairs): 
    if mapId==0:
        pairs.next() 
    import csv
    reader = csv.reader(pairs)
    for row in reader:
        date = row[3].split(' ')[0]
        time = row[3].split(' ')[1].split('+')[0]
        station = row[6]
        bikeId = row[13]
        if '2015-02-01' in date and station == 'Greenwich Ave & 8 Ave':
            yield Row(bikeId, time)
            
def getTrips(mapId, pairs):
    if mapId==0:
        pairs.next()
    import csv
    reader = csv.reader(pairs)
    for row in reader:
        import pyproj
        proj = pyproj.Proj(init = 'EPSG:2263', preserve_units = True)
        if row[5] != 'NULL' and row[5] != 0:
            time, lonlat = row[0].split(' ')[1].split('.')[0], proj(row[5], row[4])
            loc = (983519.0693404069, 208520.40726307002)
            dist = ((lonlat[0] - loc[0]) ** 2 + (lonlat[1] - loc[1]) ** 2) ** 0.5
            if dist < 1320.0: # 0.25 miles is 1320 feet
                yield Row(time)
                
def main(sc):
    spark = HiveContext(sc)

    bike = sc.textFile(citi, use_unicode=False).cache()
    taxi = sc.textFile(yellow, use_unicode=False).cache()

    brides = bike.mapPartitionsWithIndex(getRides)
    trips = taxi.mapPartitionsWithIndex(getTrips) 

    brides_df = brides.toDF(['id', 'rides'])
    trips_df = ttrips.toDF(['trips'])

    times = "HH:mm:ss"
    interval = unix_timestamp(brides_df.rides, format = times) - unix_timestamp(trips_df.trips, format = times)

    ridesTrips = brides_df.join(trips_df).filter((interval > 0) & (interval < 600)).select('id')

    print(ridesTrips.dropDuplicates().count())
    
if __name__ == "__main__":
    sc = SparkContext()
    main(sc)