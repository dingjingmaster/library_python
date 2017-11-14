#!/user/bin/env python
# -*- coding=utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

# spark 初始化
def spark_init(master, memory, cores, programName):

	conf = SparkConf().setMaster(master)\
		.set("spark.executor.memory", memory)\
		.set("spark.cores.max", cores)\
		.setAppName(programName)
	sc = SparkContext(conf = conf)

	return sc

# spark 获取数据
def spark_get_data(sc, path):

	return sc.textFile(path)

# spark map 解析hadoop字段 (gid \t key \t value ...)
def spark_parse_item(rdd, keyList):

	rdd = rdd.strip("\n")
	data = rdd.split("\t")
	gid = arr[0]
	value = []

	if 0 == len(keyList):
		return ""

	for key in keyList:
		for i in range(1, len(data), 2):
			if key == data[i]:
				value.append(data[i + 1])
				break

	return (gid, value)

