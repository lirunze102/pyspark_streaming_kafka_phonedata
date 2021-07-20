# 第一种，使用pyspark.streaming.kafka
# 包中的
# KafkaUtils实现实时读取，读取到的数据再通过spark
# sql语句进行数据处理与分析，此方法最终使用
# matplotlib绘图工具进行报表，总结以下优缺点：
# 优点： matplotlib绘图工具python安装moudle后可直接进行操作，streaming结合spark
# sql语句与
# matplotlib绘图工具易于实现，方式简单，不容易出错。
# 缺点：matplotlib绘图工具版型过于单调，在如今的大环境下显得过于死板，机制迟钝， streaming
# KafkaUtils接口方法运算代价高，在非专业设备配置的环境下所谓的实时难以实现，高延迟，高占用，总之不尽如意。
# 实现：实时数据分析报表
from pyspark import SparkContext, Row, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import json
import datetime
import time
import pandas as pd
from pandas import Series, DataFrame
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import matplotlib

matplotlib.rc("font", family='AR PL UKai CN')
from pyecharts.charts import Bar

lasttime = datetime.datetime.now()


def updatelasttime():
    global lasttime
    lasttime = datetime.datetime.now()
    # print("开始时间",lasttime)


def outputnowtime():
    global nowtime
    nowtime = datetime.datetime.now()
    print("开始时间", lasttime)
    print("结束时间", nowtime)


def updatetime():
    outputnowtime()
    updatelasttime()


# 实时分析
def realtime_output(rdd):
    infos = spark.createDataFrame(rdd)

    df1 = infos.where("a1_callerflag=0")
    df2 = infos.where("a1_callerflag=1")
    dfin_1 = df1.groupby("a4_city2").count().toDF("city", "num")
    dfin_2 = df2.groupby("a3_city1").count().toDF("city", "num")
    dfout_1 = df1.groupby("a3_city1").count().toDF("city", "num")
    dfout_2 = df2.groupby("a4_city2").count().toDF("city", "num")
    dfin = dfin_1.union(dfin_2).groupBy("city").sum("num").toDF("city", "in")
    dfout = dfout_1.union(dfout_2).groupBy("city").sum("num").toDF("city", "out")
    dfin.createOrReplaceTempView("city_in")
    dfout.createOrReplaceTempView("city_out")
    dfin_result = spark.sql('select * from city_in order by in desc')
    dfout_result = spark.sql('select * from city_out order by out desc')
    dfin_result = dfin_result.limit(10)
    dfout_result = dfout_result.limit(10)
    ins = dfin_result.toPandas()
    outs = dfout_result.toPandas()
    ins_name = list(np.array(ins['city']))
    ins_num = list(np.array(ins['in']))
    outs_name = list(np.array(outs['city']))
    outs_num = list(np.array(outs['out']))

    plt.clf()
    plt.figure(figsize=(16, 8))
    plt.subplot(1, 2, 1)
    x1 = plt.barh(ins_name, ins_num)
    plt.xlabel("呼入人次")
    plt.legend()
    plt.subplot(1, 2, 2)
    x2 = plt.barh(outs_name, outs_num)
    plt.xlabel("呼出人次")
    plt.legend()
    plt.show()

    # print(ins_name,ins_num)
    # print(outs_name,outs_num)

    # updatetime()


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
    # 实时分析数据入口
    realtime = kafka_streaming_rdd.map(lambda x: json.loads(x[1])).map(lambda x: Row(
        a1_callerflag=x['callerflag'],
        a2_timespan=x['timespan'],
        a3_city1=x['caller1']['caller1_site'],
        a4_city2=x['caller2']['caller2_site']
    )).foreachRDD(lambda x: realtime_output(x))
    ssc.start()
    ssc.awaitTermination()