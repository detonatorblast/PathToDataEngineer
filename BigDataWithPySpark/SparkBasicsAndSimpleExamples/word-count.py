from pyspark import SparkContext, SparkConf

sconf = SparkConf().setMaster("local").setAppName("wordCount")

sc = SparkContext(conf=sconf)

lines = sc.textFile("/home/muktesh/Datasets/book.txt")

# First approach
#wordsCount = lines.flatMap(lambda x: x.split()).map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y)
#print(wordsCount.collect())

# Second approach
words = lines.flatMap(lambda x: x.split())
wordscount = words.countByValue()

for word, count in wordscount.items():
    print(word, count)


