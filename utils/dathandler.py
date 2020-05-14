from pandas.io.sql import SQLTable

def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))

SQLTable._execute_insert = _execute_insert

import pandas as pd

import sys, pandas as pd
import os, os.path
import numpy as np
# import utils.arcnah as arcno
# from utils.dathandler import datReader
from sqlalchemy import *

# path = r"C:\Users\kbonefont\Desktop\MetData\Akron\AkronTable1.dat"
# engine = create_engine(os.environ['DBSTR'])
# d = datReader(path)
# d.arrays
# df = d.getdf()
# df.to_sql('test_2', schema='metdb', con=engine, index=False)
#
# lst = ['a_1', 'b_%']

from tqdm import tqdm
#
# mini1 = df[:500].copy(deep=True)
# mini
# wrapper(df, 'Akron')
#
# wrapper(mini1, 'akrontest2')

class datReader:
    arrays = []
    path = None
    header = None
    df = None
    engine = create_engine(os.environ['DBSTR'])
    correct_cols =['TIMESTAMP', 'RECORD', 'Switch', 'AvgTemp_10M_DegC', 'AvgTemp_4M_DegC',
       'AvgTemp_2M_DegC', 'AvgRH_4m_perc', 'Total_Rain_mm', 'WindDir_deg',
       'MaxWS6_10M_m/s', 'MaxWS5_5M_m/s', 'MaxWS4_2.5M_m/s', 'MaxWS3_1.5M_m/s',
       'MaxWS2_1M_m/s', 'MaxWS1_0.5M_m/s', 'StdDevWS2_1M_m/s',
       'AvgWS6_10M_m/s', 'AvgWS5_5M_m/s', 'AvgWS4_2.5M_m/s', 'AvgWS3_1.5M_m/s',
       'AvgWS2_1M_m/s', 'AvgWS1_0.5M_m/s', 'Sensit_Tot', 'SenSec']

    def __init__(self, path):
        """
        prepping the multirow header
        1. reads the first 4 lines of the .dat file
        2. gets each line into an list of lists
        3. appends 16 spaces to the first list
        4. create tuples with the list of lists
        5. create a multiindex with those tuples
        6. append created header to headless df

        """
        self.path = None
        self.arrays = []
        self.header = None
        self.df = None
        with open(path, 'r') as reader:
            all_lines = reader.readlines()
            for each_line in all_lines[:4]:
                split_line = each_line.split(",")
                self.arrays.append([each_character.replace("\"","") for each_character in split_line])
        self.path = path
        while len(self.arrays[0])<25:
            temp_space = ''
            self.arrays[0].append(temp_space)

        self.arrays = self.arrays[1]
        for n,i in enumerate(self.arrays):
            if '%' in self.arrays[n]:
                self.arrays[n]=i.replace('%', 'perc')


        self.df = pd.read_table(path, sep=",", skiprows=4, low_memory=False)

        self.df.columns = self.arrays




        # # self.getdf()

    def getdf(self):
        # with self.engine.connect() as con:
        #     cols = []
        #     res = con.execute(f'SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = \'{"MET_data"}\';')
        #     for i in res:
        #         print(i)
        #         cols.append(i[0])
        if 'JER' in self.path:
            print('jer in path')
            # for i in self.df.columns:
            #
            #     if (i not in self.correct_cols) and (i not in cols):
            #         self.correct_cols.append(i)
            #         self._adding_column(i)
            #     else:
            #         print("skipped adding columns as they were already in postgres")
            #         pass
            return self.df
        else:
            print(f'not jer, {self.path} instead')
            for i in range(0,len(self.df.columns)):
                if (self.df.columns[i]!=self.correct_cols[i])==True:
                    self.df.rename(columns={f"{self.df.columns[i]}":f"{self.correct_cols[i]}"}, inplace=True)
            return self.df

    def send(self,seldf,tablename):

        df = seldf
        # def chunker(seq, size):
        #     return (seq[pos:pos + size] for pos in range(0, len(seq), size))
        try:
            print(f" reading...{os.path.splitext(os.path.basename(self.path))[0]}")
            # chunksize = int(len(df)/10)
            # tqdm.write(f'sending {tablename}')
            #
            # with tqdm(total=len(df)) as pbar:
            #     for i, cdf in enumerate(chunker(df, chunksize)):
            #         # replace = "replace" if i == 0 else "append"

            df.to_sql(name=f'{tablename}',schema='metdb', con=self.engine, index=False, if_exists="append", method="multi")
                    # pbar.update(chunksize)
                    # tqdm._instances.clear()
            print(f" finished with...{os.path.splitext(os.path.basename(self.path))[0]}")
        except Exception as e:
            print(e)

    def _adding_column(self,column):
        with self.engine.connect() as con:
            statement = con.execute(f'ALTER TABLE gisdb.metdb."MET_data" ADD COLUMN "{column}" NUMERIC;')
