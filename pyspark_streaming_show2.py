# 实现数据实时分析的第二种方法，使用kafka模块中的KafkaConsumer消费者端口，访问十分迅速且延迟很低，并可直接使用python语言进行数据清理，使用相比第一种方法更加稳定，快速，运算代价小，易于数据处理，之后使用微型python框架——flask结合pyecharts模块方法，实现数据的实时分析，真正做到了分析报表随着数据的获取而优美顺滑的实时变动。
# 优点：该实现方法相较于第一种，只有优点没有缺点，无论数据获取，还是数据处理与报表分析，该方法均可轻易碾压第一种，唯一略有不足之处便是在选择将数据存入hive而不是进行实时报表分析时，第一种方法中的KafkaUtils才有立足之处。
# by:lirunze102
# 实现：flask微内核框架搭建的web网页中实现城市呼入呼出数据实时报表分析（真正的实时！）

from random import randrange
import ast
from pyspark import SparkContext
from pyspark import SparkConf
from kafka import KafkaConsumer
from pyspark.streaming import StreamingContext
from flask import Flask, render_template
from flask.json import jsonify
from pyecharts import options as opts
from pyecharts.charts import Bar
from pyecharts.charts import Line
import ast
from pyspark import SparkContext
from pyspark import SparkConf
from kafka import KafkaConsumer
from pyspark.streaming import StreamingContext

app = Flask(__name__, static_folder="templates")


def bar_base() -> Bar:
    vote = 0
    for msg in consumer:
        vote += 1
        data = msg.value.decode('utf-8')
        res = ast.literal_eval(data)
        data1 = res['caller1']
        data2 = res['caller2']
        try:
            if res['timespan'] == '0' or (int(res['callerflag']) != 0 and int(res['callerflag']) != 1) or int(
                    res['timespan']) > 10000:
                continue
            if len(data1['phone']) != 11 or data1['phone'] == 'NULL' or data1['caller1_site'] == 'NULL' or data1[
                'offsets'] == 'NULL' or len(data1['offsets']) >= 41:
                continue
            if len(data2['phone']) != 11 or data2['phone'] == 'NULL' or data2['caller2_site'] == 'NULL' or data2[
                'offsets'] == 'NULL' or len(data2['offsets']) >= 41:
                continue
            if res['callerflag'] == 1:
                if citysin.get(data1['caller1_site']) == None:
                    citysin[data1['caller1_site']] = 1
                else:
                    citysin[data1['caller1_site']] = citysin[data1['caller1_site']] + 1
                if citysout.get(data2['caller2_site']) == None:
                    citysout[data2['caller2_site']] = 1
                else:
                    citysout[data2['caller2_site']] = citysout[data2['caller2_site']] + 1
            else:
                if citysout.get(data1['caller1_site']) == None:
                    citysout[data1['caller1_site']] = 1
                else:
                    citysout[data1['caller1_site']] = citysout[data1['caller1_site']] + 1
                if citysin.get(data2['caller2_site']) == None:
                    citysin[data2['caller2_site']] = 1
                else:
                    citysin[data2['caller2_site']] = citysin[data2['caller2_site']] + 1
            ins = sorted(citysin.items(), key=lambda x: x[1], reverse=True)
            outs = sorted(citysout.items(), key=lambda x: x[1], reverse=True)
            num1 = 0
            name_in = []
            num_in = []
            for a in ins:
                num1 += 1
                if num1 > 10:
                    break
                name_in.append(a[0])
                num_in.append(a[1])
            # num2 = 0
            # name_out=[]
            # num_out=[]
            # for a in outs:
            #     num2+=1
            #     if num2 > 10:
            #         break
            #     name_out.append(a[0])
            #     num_out.append(a[1])
        except:
            print("未知错误，已略过")
            continue
        if vote % 250 == 0:
            c = (
                Bar()
                    .add_xaxis(name_in)
                    .add_yaxis("呼入次数", num_in)
                    .add_yaxis("", num_in, is_selected=False)
                    .set_global_opts(title_opts=opts.TitleOpts(title="城市实时呼入", subtitle="单位：次"),
                                     xaxis_opts=opts.AxisOpts(name="", axislabel_opts={"rotate": 45}))
                # Bar()
                #     .add_xaxis(name_out)
                #     .add_yaxis("呼入次数", num_out)
                #     .add_yaxis("", num_out, is_selected=False)
                #     .set_global_opts(title_opts=opts.TitleOpts(title="城市实时呼出", subtitle="单位：次"),
                #                      xaxis_opts=opts.AxisOpts(name="", axislabel_opts={"rotate": 45}))
            )
            return c


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/barChart")
def get_bar_chart():
    c = bar_base()
    return c.dump_options_with_quotes()


if __name__ == "__main__":
    citysin = {}
    citysout = {}
    consumer = KafkaConsumer('telecom', \
                             bootstrap_servers=['10.132.221.111:6667', '10.132.221.112:6667', '10.132.221.113:6667',
                                                '10.132.221.114:6667', '10.132.221.116:6667', '10.132.221.117:6667',
                                                '10.132.221.118:6667', '10.132.221.119:6667', '10.132.221.120:6667',
                                                '10.132.221.121:6667', '10.132.221.123:6667', '10.132.221.124:6667',
                                                '10.132.221.125:6667', '10.132.221.126:6667', '10.132.221.127:6667',
                                                '10.132.221.128:6667', '10.132.221.129:6667', '10.132.221.130:6667',
                                                '10.132.221.132:6667'])

    app.run()