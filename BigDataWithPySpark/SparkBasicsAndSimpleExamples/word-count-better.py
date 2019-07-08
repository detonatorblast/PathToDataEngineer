import re
from pyspark import SparkContext, SparkConf

def normalizeWords(line):
    return re.compile(r'\W+').split(line.lower())

sconf = SparkConf().setMaster("local").setAppName("wordCount")

sc = SparkContext(conf=sconf)

lines = sc.textFile("/home/muktesh/Datasets/book.txt")

# First approach
#wordsCount = lines.flatMap(lambda x: x.split()).map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y)
wordsCount = lines.flatMap(lambda x: normalizeWords(x)).map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y)
wordsCountSorted = wordsCount.map(lambda x: (x[1], x[0])).sortByKey()
#print(wordsCount.collect())
#print(wordsCountSorted.collect())

for count, word in wordsCountSorted.collect():
    print(word, count)

