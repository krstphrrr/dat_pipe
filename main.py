
import sys, pandas as pd
import os, os.path
import numpy as np
import utils.arcnah as arcno
from utils.dathandler import datReader
from sqlalchemy import *
from tqdm import tqdm
import xml.etree.ElementTree as et

#
# file = r"C:\Users\kbonefont\Downloads\Jornada Experimental Range, NM (LTAR).xml"
# xtree = et.parse(file)
#
# xroot = xtree.getroot()
# for i in range(0,10):
#     print(xroot.findall("./eainfo/detailed/attr/attrlabl")[i].text)

def pathfinder(tablename):
    for folder in os.listdir(dir_upper):
        dir_low = os.path.join(dir_upper, folder)
        for item in os.listdir(dir_low):
            if tablename in item:
                final_path = os.path.normpath(os.path.join(dir_low,item))
                return final_path

adding_column('SWUpper_Avg')

# for node in xroot.findall('idinfo'):
#     print(node.tag)
#     # if 'idinfo' in node.tag:
#     #     for child in node.attrib:
#     #         print(child,"A")
# s_idinfo = node.attrib.get("idinfo")
# s_dataqual = node.find("dataqual").text
os.listdir(dir_upper)
#
pd.read_table(pathfinder('Pullman'),skiprows=4,low_memory=False)

import numpy as np
np.loadtxt(pathfinder('Moab'))
#
pathfinder('BigSpring')
del(test)
test = datReader(pathfinder('Moab'))
t =test.getdf()
test.send(t[:30], "test")

for i in t.columns:
    if i not in correct_cols:
        print(i)

test.send(t[:30],"test")
# correct_cols =['TIMESTAMP', 'RECORD', 'Switch', 'AvgTemp_10M_DegC', 'AvgTemp_4M_DegC',
#    'AvgTemp_2M_DegC', 'AvgRH_4m_perc', 'Total_Rain_mm', 'WindDir_deg',
#    'MaxWS6_10M_m/s', 'MaxWS5_5M_m/s', 'MaxWS4_2.5M_m/s', 'MaxWS3_1.5M_m/s',
#    'MaxWS2_1M_m/s', 'MaxWS1_0.5M_m/s', 'StdDevWS2_1M_m/s',
#    'AvgWS6_10M_m/s', 'AvgWS5_5M_m/s', 'AvgWS4_2.5M_m/s', 'AvgWS3_1.5M_m/s',
#    'AvgWS2_1M_m/s', 'AvgWS1_0.5M_m/s', 'Sensit_Tot', 'SenSec']
#
# # t.columns[23]!=correct_cols[23]
# for i in range(0,len(t.columns)):
#     if (t.columns[i]!=correct_cols[i])==True:
#         t.rename(columns={f"{t.columns[i]}":f"{correct_cols[i]}"})
#

"""
# TODO:
- handling multirow header in ingest to pg
"""
# path = r"C:\Users\kbonefont\Desktop\MetData\Akron\AkronTable1.dat"
# direc =  r"C:\Users\kbonefont\Desktop\MetData\Akron"
#
# d = datReader(path)
# df = d.getdf()
#
# df.to_sql('test_akron', schema='metdb',con=engine)
#
# df.shape
# df['TIMESTAMP']
# def fun1(arg1,arg2):
#     arg1+='arg1'
#     arg2+='arg2'
#     return arg1,arg2
# (var1, var2) =fun1('a_','b_')
# var1
# var2
engine = create_engine(os.environ['DBSTR'])
with engine.connect() as con:
    cols = []
    res = con.execute(f'SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = \'{"MET_data"}\';')
    for i in res:
        cols.append(i[0])

dir_upper =  r"C:\Users\kbonefont\Desktop\MetData"
# os.listdir(dir_upper)
# dir_lower = os.path.join()
# arc = arcno
# "a ranch is missing, heartrock . hdrt, "
# "bellvue "
# dir_lower = os.path.join(dir_upper, os.listdir(dir_upper)[0])
# final_dir = os.path.join(dir_lower, os.listdir(dir_lower)[0])
# a = datReader(final_dir)
# akron = a.getdf()
# akron['ProjectKey'] = "Akron"
# akron[:5]
# a.send(akron,'Met_test')
# def order_check():
#     check=1
#     while check==1:
#         print("voy a hacer que esto vaya primero")
#         check=2
#     else:
#         print("y esto segundo")

# order_check()
os.listdir(dir_upper)
dir_upper =  r"C:\Users\kbonefont\Desktop\MetData"
name = "JER"
test = datReader(pathfinder(name))
# df.iloc[:,-1]

df = test.getdf().copy(deep=True)
df['ProjectKey'] = name

df.shape
test.send(seldf = df,tablename="MET_data")
for i in df.columns:
    if '\n' in i:
        print(i)
        df.rename(columns={f'{i}':'{0}'.format(i.replace("\n",""))}, inplace=True)


def iterator():

    for folder in os.listdir(dir_upper):
        projectname = folder
        dir_low = os.path.join(dir_upper, folder)
        for item in tqdm(os.listdir(dir_low)):
            final_path = os.path.normpath(os.path.join(dir_low,item))
            if '1' in item:
                #instantiate
                # print(final_path)
                inst = datReader(final_path)
                #get df
                df = inst.getdf().copy(deep=True)
                # #prepdf
                df = df.copy(deep=True)
                df['ProjectKey'] = projectname

                # # send
                inst.send(seldf=df,tablename="MET_data")



iterator()
# s = set()
# for i in t.columns:
#     if (i not in s) and i not in correct_cols:
#         s.add(i)


# engine = create_engine(os.environ['DBSTR'])
# def adding_column(column):
#     with engine.connect() as con:
#         statement = con.execute(f'ALTER TABLE gisdb.metdb."test" ADD COLUMN "{column}" NUMERIC;')



# for dr in os.listdir(dir_upper):
#     # print(dr)
#     if 'Akr' in dr:
#         name = 'Akron'
#         arc = arcno
#
#         dir_lower = os.path.normpath(os.path.join(dir_upper, dr))
#         # print(dir_lower)
#         for i in os.listdir(dir_lower):
#             # print(i)
#             if os.path.splitext(i)[1]=='.dat':
#                 # print(i)
#                 path_to_send = os.path.join(dir_lower,i)
#
#                 if "" in i:
#                     a = datReader(os.path.join(dir_lower,i))
#
# #
#
# # indexing strategies in the age of multirow headers
# a.df



























/
