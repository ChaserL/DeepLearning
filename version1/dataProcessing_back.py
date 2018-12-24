# -*- coding: utf-8 -*-
"""
Created on Fri Nov  2 10:34:51 2018

@author: Ramon
"""


import happybase
import pymssql
import time
import pandas as pd
from collections import OrderedDict
import decimal
import datetime
import time
from functools import wraps

'''
程序运行时间
'''
def fn_timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        print ("Total time running %s: %s seconds" %(function.func_name, str(t1-t0)))
        return (result)
    return (function_timer)
'''
13位时间戳互转
'''
class timeStamp():
    def timeStamp(timeNum):
        timeStamp = float(timeNum/1000)
        timeArray = time.localtime(timeStamp)
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        return otherStyleTime
    def timeStampReverse(timeStr):
        long_max = 9223372036854775807
        timeArray = time.strptime(timeStr, "%Y-%m-%d %H:%M:%S")
        otherStyleTime = str(long_max - int(round(time.mktime(timeArray) * 1000)))
        return (otherStyleTime)
'''
数据格式处理
'''
class dataTransferType():
    def byteToStr(df):
        for value in df:
            for row in range(len(df)):
                if(pd.isnull(df[value][row])==True):
                    pass
                else:
                    df[value][row] = df[value][row].decode()
        return df
    def strToTime(series):
        series = pd.to_datetime(series,format = "%Y-%m-%d %H:%M:%S")
        return series
'''
截出ROWKEY里的时间 map(def,data)，此部分处理的是ROWKEY和VALUE
'''
class dealKeyValue():
    def cutRow(strs,_type = 'time'):
        strs = str(strs)
        if _type == 'time':
            strs = int(strs[11:-5])
        if _type == 'plaza':
            strs = str(strs[4:10])
        if _type == 'alarm_type':
            strs = int(strs[2])
        return strs
    def cutValue(strs,_type = 'colnames'):
        if _type == 'colnames':
            strs = str(strs[7:-1])
        if _type == 'values':
            strs = str(strs[2:-1])
        if _type == 'dfColumns':
            strs = str(strs[5:])
        return strs
'''
处理迭代器，生成最终结构化 可以处理的dataframe
'''
class toDf(dealKeyValue):
    def dealIterator(iterator):
        ll = []
        for key,value in iterator:
#            v = list(map(dealKeyValue.cutValue,list(map(str,list(value.values()))),['values']*len(value.values())))
            v = list(value.values())
            ll.append(v)
        k = list(map(dealKeyValue.cutValue,list(map(str,list(value.keys())))))
        if len(k) != len(max(ll)):
            t = abs(len(k) - len(max(ll)))
            for i in range(t):
              tmp = str(str(i) + 'replenish')
              k.append(tmp)
        df = pd.DataFrame(data = ll,columns = k)
        return df
    def dealDict(dictData):
        df = pd.DataFrame.from_dict(dictData,orient='index').T
        for i in range(len(df.columns)):#处理columns
            df.columns.values[i] = dealKeyValue.cutValue(df.columns.values[i].decode(),'dfColumns')
        df = dataTransferType.byteToStr(df) #处理values
        return df
'''
连接HBASE按需求扫描表、连接SQLSERVER获取对应关系表
'''
class connAndScan(toDf):
    def connHbase(_IP = '192.168.1.27',_tableName = 'AlarmRecordHistory',_row_start = '',_row_stop = ''):
        if _row_start == '' or _row_stop =='':
            return('NULL')
        else:
            conn = happybase.Connection(_IP)
            table = conn.table(_tableName)
            #row = table.row('1|AHAQWY|9223370497722576554|3|0') #按ROW去抽取数据
            query = table.scan(row_start=_row_start,row_stop=_row_stop) #limit = 10
            conn.close() #不关闭下次连接会报错
            df = toDf.dealIterator(query)
            #result = next(query)
            return df
    def connSqlserver(_host = '192.168.1.26', _user = 'sa', _password = 'dahaiYF123', _database = 'XCXYPTDB', _charset="utf8"):
        conn = pymssql.connect(host = _host, user = _user, password = _password, database = _database, charset= _charset)
        cur = conn.cursor()
        sql = 'SELECT * FROM BasicInfo'
        cur.execute(sql)
        ind = cur.description
        rs = cur.fetchall()
        d = OrderedDict()
        for r in rs:
            i = 0
            name_list = []
            for name in ind:
                if name[0] not in name_list:
                    name_list.append(name[0])
                else:
                    continue
                if name[0] not in d:
                    d[name[0]] = []
                v = float(r[i]) if type(r[i]) is decimal.Decimal else r[i]
                
                v = str(v) if type(v) is datetime.datetime else v
                d[name[0]].append(v)
                i += 1
        df = pd.DataFrame(d)
        return (df)



#初始化变量及环境
IP = '192.168.1.27'
table = 'AlarmRecordHistory'
plaza = 'AHAQWY'
startTime = '2018-10-01 00:00:00'
endTime = '2018-11-01 00:00:00'
startRowkey = '1|' + plaza + '|' + timeStamp.timeStampReverse(startTime)
endRowkey = '1|' + plaza + '|' + timeStamp.timeStampReverse(endTime)

#连接数据库,获取数据
conn = happybase.Connection(IP)
table = conn.table(table)
iterator = table.scan(row_start=endRowkey,row_stop=startRowkey)
df = toDf.dealIterator(iterator)
df = dataTransferType.byteToStr(df)
conn.close()
ddd = connAndScan.connSqlserver()

#数据预处理
deviceID = df['deviceID'].drop_duplicates()
df.columns.values
df['dealTime1'] = dataTransferType.strToTime(df['dealTime'])
df['alarmTime1'] = dataTransferType.strToTime(df['alarmTime'])
#data_ = data_.drop(['deviceID1'],axis=1)
tmp = pd.DataFrame(df['deviceID'].groupby(by = df['deviceID']).count())
tmp = tmp.rename(columns={'deviceID':'times'})
tmp['deviceID'] = tmp.index.values
data_ = pd.DataFrame([])
data_['alarmTypeID1'] = df['alarmTypeID']
data_['alarmlevelID1'] = df['alarmlevelID']
data_['deviceID'] = df['deviceID']
data_['systemID1'] = df['systemID']
data_['dealTime1'] = (dataTransferType.strToTime(df['dealTime']) - dataTransferType.strToTime('2018-10-01 00:00:00')).dt.total_seconds()
data_['alarmTime1'] = (dataTransferType.strToTime(df['alarmTime']) - dataTransferType.strToTime('2018-10-01 00:00:00')).dt.total_seconds()
data_['dealz'] = (dataTransferType.strToTime(df['dealTime']) - dataTransferType.strToTime(df['alarmTime'])).dt.total_seconds()
data_ = pd.merge(data_,tmp)


#搭建模型
from sklearn.cluster import KMeans
num_clusters = 3
km = KMeans(n_clusters=num_clusters)
km.fit(data_.drop(['deviceID','dealTime1'],axis=1))
#km.fit(data_)
#data_['labels'] = km.labels_
df['labels'] = km.labels_
df['dealz'] = data_['dealz']
df['times'] = data_['times']
df['alarmTimeStamp'] = df['alarmTime'].apply(lambda x:int(time.mktime((time.strptime(x,"%Y-%m-%d %H:%M:%S")))))

# ttmp = pd.DataFrame(df['times'].groupby([df['labels']]).mean())
# ttmp['dealz'] = pd.DataFrame(df['dealz'].groupby([df['labels']]).mean())


#绘制图片
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
ax = plt.subplot(111, projection='3d')  # 创建一个三维的绘图工程,将数据点分成三部分画，在颜色上有区分度
ax.scatter(df['alarmTimeStamp'][df['labels']==0], df['dealz'][df['labels']==0], df['times'][df['labels']==0], c='y')  # 绘制数据点
ax.scatter(df['alarmTimeStamp'][df['labels']==1], df['dealz'][df['labels']==1], df['times'][df['labels']==1], c='r')  # 绘制数据点
ax.scatter(df['alarmTimeStamp'][df['labels']==2], df['dealz'][df['labels']==2], df['times'][df['labels']==2], c='g')  # 绘制数据点
plt.show()
ax.set_zlabel('Z') 
ax.set_ylabel('Y')
ax.set_xlabel('X')
plt.show()
