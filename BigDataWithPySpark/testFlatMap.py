# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf

sconf = SparkConf().setMaster("local").setAppName("TestApp")

sc = SparkContext(conf=sconf)

rdd = sc.textFile("/home/muktesh/Datasets/allAlphabets.txt")

uppercaseMap = rdd.map(lambda x: x.split()) # Will return lists of words for each line
uppercaseFlatMap = rdd.flatMap(lambda x: x.split()) # Will return one list of all words in the file

print(uppercaseMap.collect())
print(uppercaseFlatMap.collect())
