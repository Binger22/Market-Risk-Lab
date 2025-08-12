#!/usr/bin/env python
# coding: utf-8

# In[16]:


import numpy as np
import pandas as pd
import pymysql
import FRTB_module
from datetime import *
import time

import warnings
warnings.filterwarnings('ignore')

# In[17]:


def main(p_data_date):

    db=pymysql.connect(host='82.156.70.141',
                       port=3306,
                       db='ry-vue',
                       user='mrdbuser',
                       password='Findeck^2022',
                       charset='utf8')

    cursor=db.cursor()

    cursor.execute("select * from TB_26_FRTB_SENSITIVITY_GROUP where DATA_DATE = %s;", p_data_date)
    db_sa=cursor.fetchall()
    db_sa_names = [i[0] for i in cursor.description]

    db.commit()
    cursor.close()
    db.close()

    Raw_Data=pd.DataFrame(db_sa,columns=db_sa_names)
    Raw_Data['DATA_DATE'] = pd.to_datetime(Raw_Data['DATA_DATE'],format='%Y-%m-%d')
    Raw_Data[['RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','SENSITIVITY_VAL_RPT_CURR_CNY']]=Raw_Data[['RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','SENSITIVITY_VAL_RPT_CURR_CNY']].apply(pd.to_numeric)

    detail = FRTB_module.exct(Raw_Data)
    pos = detail[0]
    bucket = detail[1]
    class_ = detail[2]
    riskfactor = detail[3]
    level3 = detail[4]
    
    pos.loc[:,'DATA_DATE']=p_data_date
    bucket.loc[:,'DATA_DATE']=p_data_date
    class_.loc[:,'DATA_DATE']=p_data_date
    riskfactor.loc[:,'DATA_DATE']=p_data_date
    level3.loc[:,'DATA_DATE']=p_data_date
    
    pos=pos.astype(object).where(pd.notnull(pos), None)
    bucket=bucket.astype(object).where(pd.notnull(bucket), None)
    class_=class_.astype(object).where(pd.notnull(class_), None)
    riskfactor=riskfactor.astype(object).where(pd.notnull(riskfactor), None)
    level3=level3.astype(object).where(pd.notnull(level3), None)
    
    db=pymysql.connect(host='82.156.70.141',
                       port=3306,
                       db='ry-vue',
                       user='mrdbuser',
                       password='Findeck^2022',
                       charset='utf8')
    # create cursor
    cursor=db.cursor()

    
    ########## result: risk factor##########
    cursor.execute("truncate table `TB_26_FRTB_SENSITIVITY_RESULT_RISKFACTOR`")
    db.commit()

    cols1 = "`,`".join(pos.columns)

    # Insert DataFrame recrds one by one.
    for i,row in pos.iterrows():
        sql1 = "INSERT INTO `TB_26_FRTB_SENSITIVITY_RESULT_RISKFACTOR` (`" +cols1 + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql1, tuple(row))
    db.commit()
    

    ########## result: bucket ##########
    cursor.execute("truncate `TB_26_FRTB_SENSITIVITY_RESULT_BUCKET`")
    db.commit()

    cols2 = "`,`".join(bucket.columns)

    # Insert DataFrame recrds one by one.
    for i,row in bucket.iterrows():
        sql2 = "INSERT INTO `TB_26_FRTB_SENSITIVITY_RESULT_BUCKET` (`" +cols2 + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql2, tuple(row))
    db.commit()

    ########## result: class ##########
    cursor.execute("truncate `TB_26_FRTB_SENSITIVITY_RESULT_RISKCLASS`")
    db.commit()

    cols3 = "`,`".join(class_.columns)

    # Insert DataFrame recrds one by one.
    for i,row in class_.iterrows():
        sql3 = "INSERT INTO `TB_26_FRTB_SENSITIVITY_RESULT_RISKCLASS` (`" +cols3 + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql3, tuple(row))
    db.commit()

    ########## decomp: risk factor##########
    cursor.execute("truncate `TB_26_FRTB_SENSITIVITY_DECOMP_RISKFACTOR`")
    db.commit()

    cols4 = "`,`".join(riskfactor.columns)

    # Insert DataFrame recrds one by one.
    for i,row in riskfactor.iterrows():
        sql4 = "INSERT INTO `TB_26_FRTB_SENSITIVITY_DECOMP_RISKFACTOR` (`" +cols4 + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql4, tuple(row))
    db.commit()

    ########## decomp: portfolio ##########
    cursor.execute("truncate `TB_26_FRTB_SENSITIVITY_DECOMP_PORTFOLIO`")
    db.commit()

    cols5 = "`,`".join(level3.columns)

    # Insert DataFrame recrds one by one.
    for i,row in level3.iterrows():
        sql5 = "INSERT INTO `TB_26_FRTB_SENSITIVITY_DECOMP_PORTFOLIO` (`" +cols5 + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql5, tuple(row))
    db.commit()

    cursor.close()
    db.close()
    


# In[18]:


if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    p_data_date=args[0]
    p_data_date=datetime.strptime(p_data_date,'%Y%m%d').strftime('%Y-%m-%d')
    main(p_data_date)

