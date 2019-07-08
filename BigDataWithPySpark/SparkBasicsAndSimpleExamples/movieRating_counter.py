from pyspark import SparkContext, SparkConf
#import collections

sconf = SparkConf().setMaster("local").setAppName("movieRatingCount")
sc = SparkContext(conf=sconf)

lines = sc.textFile("/home/muktesh/Datasets/ml-100k/u.data")
ratings = lines.map(lambda x: (x.split()[2], 1)).reduceByKey(lambda x,y: x + y)
result = ratings.collect()
for r in result:
    print(r)

