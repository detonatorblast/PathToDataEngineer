from pyspark import SparkConf, SparkContext

sconf = SparkConf().setMaster("local").setAppName("Popular movie")

sc = SparkContext(conf = sconf)

movies = sc.textFile("/home/muktesh/Datasets/ml-100k/u.data")
moviecounts = movies.map(lambda x: (x.split()[1], 1)).reduceByKey(lambda x, y: x + y)
moviecountsorted = moviecounts.map(lambda x: (x[1], x[0])).sortByKey()

for movie in moviecountsorted.collect():
    print(movie)
