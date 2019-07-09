from pyspark import SparkConf, SparkContext
import codecs

sconf = SparkConf().setMaster("local").setAppName("PopularMoviesWithName")

sc = SparkContext(conf = sconf)


def loadmovienames():
    namedict = {}
    fp = codecs.open("/home/muktesh/Datasets/ml-100k/u.item", encoding='utf-8', errors='ignore')
    for line in fp:
        record = line.split('|')
        namedict[record[0]] = record[1]
    return namedict

namedict = sc.broadcast(loadmovienames())

movies = sc.textFile("/home/muktesh/Datasets/ml-100k/u.data")
moviescount = movies.map(lambda x: (x.split()[1], 1)).reduceByKey(lambda x, y: x + y)
sortedmoviecount = moviescount.map(lambda x: (x[1], x[0])).sortByKey()

movieswithnames = sortedmoviecount.map(lambda x: (namedict.value[x[1]], x[0]))

for key, val in movieswithnames.collect():
    print(key, val)
