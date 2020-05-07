
import sys, pandas as pd
import os, os.path
import numpy as np
import utils.arcnah as arcno
from utils.dathandler import datReader

"""
# TODO:
- handling multirow header in ingest to pg
"""

# def fun1(arg1,arg2):
#     arg1+='arg1'
#     arg2+='arg2'
#     return arg1,arg2
# (var1, var2) =fun1('a_','b_')
# var1
# var2
dir_upper = r""
# dir_lower = os.path.join()
for dr in os.listdir(dir):
    # if 'Akron' in dr: # has to be list of allowed values/ or disallowed ones
    #     dir_lower = os.path.join(dir_upper, dr)
    #     for i in os.listdir(dir_lower):
    #         if os.path.splitext(i)[1]=='.dat':
    #             path_to_send = os.path.join(dir_lower,i)
    #             print(path_to_send)
    #             # datreader processes,
    #             # dathandler would get it ready.
    #             a = datReader(os.path.join(dir_lower,i))
    if 'JER' in dr:
        dir_lower = os.path.join(dir_upper, dr)
        for i in os.listdir(dir_lower):
            if os.path.splitext(i)[1]=='.dat':
                path_to_send = os.path.join(dir_lower,i)
                if "DustTrak" in i:
                    a = datReader(os.path.join(dir_lower,i))



# indexing strategies in the age of multirow headers
a.df



























/
