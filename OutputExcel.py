import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import numpy as np
import glob
import sqlalchemy
from sqlalchemy import create_engine,select, update, delete, values
import time
import os
from datetime import datetime

database_username = 'root'
database_password = ''
database_ip       = 'localhost:3306'
database_name     = 'fastapi'
database_connection = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))


df_true = pd.read_excel('20220708.xlsx')

df_true.rename(columns = {'料號':'partNumber', '品名':'productName','規格':'spec','年度':'year','月份':'month',
                        '\t上月結存數量':'lastStock','本月入庫數量':'totalIn','本月重工領出數量':'reworkOut',
                        '本月工單領用數量':'orderOut','本月雜項領用數量':'sundryOut','本月其他調整數量':'otherOut','本月結存數量':'stock',
                         '安全庫存':'safety'}, inplace = True)

df_true['true_consumption'] = (df_true['reworkOut']+df_true['orderOut']+ df_true['sundryOut']+df_true['otherOut']).abs()

predictMonth1 = df_true[df_true['month']==7][['partNumber','true_consumption']]
predictMonth1.rename(columns = {'true_consumption':'true_consumption1'}, inplace = True)
predictMonth2 = df_true[df_true['month']==8][['partNumber','true_consumption']]
predictMonth2.rename(columns = {'true_consumption':'true_consumption1'}, inplace = True)

df11 = pd.read_sql_table(table_name = 'toyo_erp_tt',con = database_connection).reset_index(drop = True)
df11 = df11.drop(['id'], axis=1)
df11 = df11[df11['modelName']=='2022-09-13-10-32-59'].reset_index(drop = True)
df111 = df11.merge(predictMonth1, on='partNumber',how="left")
df111 = df111.merge(predictMonth2, on='partNumber',how="left")

df111.to_csv('answer.csv',index = False,encoding='utf_8_sig')