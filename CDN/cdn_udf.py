import re

import json
from pyflink.table import DataTypes
from pyflink.table.udf import udf
from urllib.parse import quote_plus
from urllib.request import urlopen


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def ip_to_province(ip):
   """
   format:
       {
       'ip': '27.184.139.25',
       'pro': '河北省',
       'proCode': '130000',
       'city': '石家庄市',
       'cityCode': '130100',
       'region': '灵寿县',
       'regionCode': '130126',
       'addr': '河北省石家庄市灵寿县 电信',
       'regionNames': '',
       'err': ''
       }
   """
   try:
       urlobj = urlopen( \
        'http://whois.pconline.com.cn/ipJson.jsp?ip=%s' % quote_plus(ip))
       data = str(urlobj.read(), "gbk")
       pos = re.search("{[^{}]+\}", data).span()
       geo_data = json.loads(data[pos[0]:pos[1]])
       if geo_data['pro']:
           return geo_data['pro']
       else:
           return geo_data['err']
   except:
       return "UnKnow"