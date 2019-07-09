from pyspark import SparkConf, SparkContext
import codecs
import re

sconf = SparkConf().setMaster("local").setAppName("Popular Super Hero")
sc = SparkContext(conf = sconf)

def loadheronames():
    heronamedict = {}

    fp = codecs.open("/home/muktesh/Datasets/Marvel-names.txt", encoding='utf-8', errors='ignore')
    for line in fp:
        record = re.compile(r'\s\W').split(line)
        #record = line.split()
        heronamedict[record[0]] = record[1].rstrip('"')

    return heronamedict

heronamedict = sc.broadcast(loadheronames())

herograph = sc.textFile("/home/muktesh/Datasets/Marvel-graph.txt")
heroappearanceCount = herograph.flatMap(lambda x: x.split()).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
heroappSortedCountaskey = heroappearanceCount.map(lambda x: (x[1], x[0])).sortByKey()

print(heronamedict.value[heroappSortedCountaskey.max()[1]])



