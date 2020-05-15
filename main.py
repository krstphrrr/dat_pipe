
import sys, pandas as pd
import os, os.path
import numpy as np
import utils.arcnah as arcno
from utils.dathandler import datReader
from utils.utils import db
from sqlalchemy import *
from tqdm import tqdm

def pathfinder(tablename):
    for folder in os.listdir(dir_upper):
        dir_low = os.path.join(dir_upper, folder)
        for item in os.listdir(dir_low):
            if tablename in item:
                final_path = os.path.normpath(os.path.join(dir_low,item))
                return final_path




dir_upper =  r"C:\Users\kbonefont\Desktop\MetData"
lst = os.listdir(dir_upper)



name = "TwinValley"
test = datReader(pathfinder(name))
# # df.iloc[:,-1]
#
df = test.getdf().copy(deep=True)
for i in df.columns:
    if '/' in i:
        df.rename(columns={f'{i}':'{0}'.format(i.replace("/","_"))}, inplace=True)
for i in df.columns:
    if '.' in i:
        df.rename(columns={f'{i}':'{0}'.format(i.replace(".",""))}, inplace=True)
df['ProjectKey'] = name

def createtable():
    command = """ CREATE TABLE gisdb.metdb."MET_data"
    (
    "TIMESTAMP" TIMESTAMP,
    "RECORD" bigint,
    "Switch" bigint,
    "AvgTemp_10M_DegC" numeric(12,6),
    "AvgTemp_4M_DegC" numeric(12,6),
    "AvgTemp_2M_DegC" numeric(12,6),
    "AvgRH_4m_perc" numeric(12,6),
    "Total_Rain_mm" numeric(12,6),
    "WindDir_deg" numeric(12,6),
    "MaxWS6_10M_m_s" numeric(12,6),
    "MaxWS5_5M_m_s" numeric(12,6),
    "MaxWS4_25M_m_s" numeric(12,6),
    "MaxWS3_15M_m_s" numeric(12,6),
    "MaxWS2_1M_m_s" numeric(12,6),
    "MaxWS1_05M_m_s" numeric(12,6),
    "StdDevWS2_1M_m_s" numeric(12,6),
    "AvgWS6_10M_m_s" numeric(12,6),
    "AvgWS5_5M_m_s" numeric(12,6),
    "AvgWS4_25M_m_s" numeric(12,6),
    "AvgWS3_15M_m_s" numeric(12,6),
    "AvgWS2_1M_m_s" numeric(12,6),
    "AvgWS1_05M_m_s" numeric(12,6),
    "Sensit_Tot" BIGINT,
    "SenSec" BIGINT,
    "SWUpper_Avg" numeric(12,6),
    "SWLower_Avg" numeric(12,6),
    "LWUpperCo_Avg" numeric(12,6),
    "LWLowerCo_Avg" numeric(12,6),
    "CNR4TK_Avg" numeric(12,6),
    "RsNet_Avg" numeric(12,6),
    "RlNet_Avg" numeric(12,6),
    "Albedo_Avg" numeric(12,6),
    "Rn_Avg" numeric(12,6),
    "ProjectKey" text
    )
    """

    con = db.str2
    cur = con.cursor()
    try:
        cur.execute(command)
        con.commit()
        # cur.execute("selec")
    except Exception as e:
        con = db.str2
        cur = con.cursor()
        print(e)
createtable()



from io import StringIO

import psycopg2
from tqdm import tqdm


def copy_from(df: pd.DataFrame,
              table: str,
              connection: psycopg2.extensions.connection,
              chunk_size: int = 10000):
    cursor = connection.cursor()
    df = df.copy()

    escaped = {'\\': '\\\\', '\n': r'\n', '\r': r'\r', '\t': r'\t',}
    for col in df.columns:
        if df.dtypes[col] == 'object':
            for v, e in escaped.items():
                df[col] = df[col].str.replace(v, e)
    try:
        for i in tqdm(range(0, df.shape[0], chunk_size)):
            f = StringIO()
            chunk = df.iloc[i:(i + chunk_size)]

            chunk.to_csv(f, index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
            f.seek(0)
            cursor.copy_from(f, table, columns=[f'"{i}"' for i in df.columns])
            connection.commit()
    except psycopg2.Error as e:
        print(e)
        connection.rollback()
    cursor.close()


copy_from(df,'gisdb.metdb."MET_data"',db.str2, 10000)
