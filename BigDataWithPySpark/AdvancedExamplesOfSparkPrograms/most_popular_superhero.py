from pyspark import SparkConf, SparkContext
import codecs
import re

sconf = SparkConf().setMaster("local").setAppName("Popular Super Hero")
sc = SparkContext(conf = sconf)

def countCooccurences(line):
    elems = line.split()
    return (elems[0], len(elems)-1)
    

def parseNames(line):
    record = re.compile(r'\s\W').split(line)
    return (record[0], record[1].rstrip('"'))

heroNamesRdd = sc.textFile("/home/muktesh/Datasets/Marvel-names.txt").map(lambda line: parseNames(line))

herograph = sc.textFile("/home/muktesh/Datasets/Marvel-graph.txt")
heroAppCount = herograph.map(lambda x: countCooccurences(x)).reduceByKey(lambda x, y: x + y)

heroAppCountasKey = heroAppCount.map(lambda x: (x[1], x[0])).max()

mostpopular = heroNamesRdd.lookup(heroAppCountasKey[1])[0]
print("Hero with most appearances is {} with {} appearance".format(str(mostpopular), heroAppCountasKey[0]))
