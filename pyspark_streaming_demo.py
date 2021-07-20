#利用pycharm链接pyspark环境，通过使用spark streaming技术中的 KafkaUtils端口将服务器上的数据进行处理并实时写入Hive。
# 实现：数据实时入库
# by:lirunze102

from pyspark import SparkContext, Row, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import json
import datetime
import time
import pandas as pd
from pandas import Series,DataFrame
import matplotlib.pyplot as plt
import numpy as np
def all_infos_tohive(rdd):
    infos = spark.createDataFrame(rdd)
    infos.createOrReplaceTempView('numbers_infos')
    x=spark.sql('select * from numbers_infos')
    x.show()
    hivepath = 'hdfs://localhost:9000/user/hive/warehouse/spark.db/data_all'
    x.write.csv(hivepath,mode='append')

if __name__ == '__main__':
    outputnowtime()
    conf = SparkConf().setAppName("DstreamDemo").set('spark.executor.memory',
                                                     '10g').set("spark.executor.cores", '8')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    ssc = StreamingContext(sc, 1)
    spark = SparkSession.builder.getOrCreate()
    brokers = '10.132.221.111:6667,10.132.221.112:6667,' \
              '10.132.221.113:6667,10.132.221.114:6667,10.132.221.116:6667,' \
              '10.132.221.117:6667,10.132.221.118:6667,10.132.221.119:6667,' \
              '10.132.221.120:6667,10.132.221.121:6667,10.132.221.123:6667,' \
              '10.132.221.124:6667,10.132.221.125:6667,10.132.221.126:6667,' \
              '10.132.221.127:6667,10.132.221.128:6667,10.132.221.129:6667,' \
              '10.132.221.130:6667,10.132.221.132:6667'
    topic = 'telecom'
    kafka_streaming_rdd = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams={'metadata.broker.list': brokers,
                                                                                        "group.id": 'lrz01'})
    # 本地存储功能
    all_infos = kafka_streaming_rdd.map(lambda x: json.loads(x[1])).map(lambda x: Row(
                                                a1_callerflag=x['callerflag'],
                                                a2_timespan=x['timespan'],
                                                a3_call1=x['caller1']['phone'],
                                                a4_city1=x['caller1']['caller1_site'],
                                                a5_call2=x['caller2']['phone'],
                                                a6_city2=x['caller2']['caller2_site']
                                                                    )).foreachRDD(lambda x: all_infos_tohive(x))
    ssc.start()
    ssc.awaitTermination()