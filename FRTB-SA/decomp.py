#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import time


# In[23]:


pd.options.display.max_rows = 10
pd.options.display.max_columns = None


# In[ ]:


start_time=time.time()


# In[2]:


import pymysql

db=pymysql.connect(host='82.156.70.141',
                   port=3306,
                   db='ry-vue',
                   user='mrdbuser',
                   password='Findeck^2022',
                   charset='utf8')

cursor=db.cursor()

sql="select * from TB_26_FRTB_SENSITIVITY_TRADE_DETAIL;"
cursor.execute(sql)
db_sa=cursor.fetchall()
db_sa_names = [i[0] for i in cursor.description]

db.commit()
cursor.close()
db.close()


# In[3]:


Raw_Data=pd.DataFrame(db_sa,columns=db_sa_names)
Raw_Data=Raw_Data.rename({'SENSITIVITY_VAL_REPORTING_CURR_CNY':'WEIGHTED_SENSITIVITY'},axis=1)
Raw_Data[['RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','WEIGHTED_SENSITIVITY']]=Raw_Data[['RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','WEIGHTED_SENSITIVITY']].apply(pd.to_numeric)


# In[ ]:





# In[4]:


params=pd.ExcelFile('params.xlsx')


# In[5]:


High_Multipler = 1.25
Low_Multipler1 = 2
Low_Multipler2 = 0.75


# In[6]:


GIRR_Rho = params.parse('GIRR_Rho')
GIRR_Diff_Mlt = 0.999
GIRR_Infl_Mlt = 0.4
GIRR_Cross_Mlt = 0
GIRR_Gamma = 0.5
GIRR_LH = 60
GIRR_vega_rw = 1


# In[7]:


#Raw_Data=pd.ExcelFile('RawData.xlsx').parse('all')


# In[8]:


GIRR_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='GIRR')]

GIRR_Position = GIRR_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_CLASS',
                              'RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE','SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]

GIRR_Position = GIRR_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2',
                                       'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE',
                                       'SENSITIVITY_TYPE']
                                      ,dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()


# In[9]:


GIRR_delta = GIRR_Position[(GIRR_Position['SENSITIVITY_TYPE']=='Delta')]


# In[10]:


GIRR_delta_kl = GIRR_delta.rename(
{'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_K'
 ,'RISK_FACTOR_ID':'RISK_FACTOR_ID_K'
 ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_K'
 ,'RISK_FACTOR_TYPE':'RISK_FACTOR_TYPE_K'},axis=1
).merge(GIRR_delta[['RISK_FACTOR_ID','RISK_FACTOR_TYPE','RISK_FACTOR_BUCKET'
                    ,'RISK_FACTOR_VERTEX_1','WEIGHTED_SENSITIVITY']]
       .rename({'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_L'
                ,'RISK_FACTOR_ID':'RISK_FACTOR_ID_L'
                ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_L'
                ,'RISK_FACTOR_TYPE':'RISK_FACTOR_TYPE_L'},axis=1)
       ,on=['RISK_FACTOR_BUCKET'],how='left')


# In[11]:


GIRR_delta_kl = GIRR_delta_kl.merge(GIRR_Rho,on=['RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_VERTEX_1_L'],how='left')
GIRR_delta_kl.rename(columns={'Rho_KL':'Rho_kl_M'},inplace=True)
GIRR_delta_kl.loc[GIRR_delta_kl.RISK_FACTOR_ID_K!=GIRR_delta_kl.RISK_FACTOR_ID_L,'Rho_kl_M']=GIRR_delta_kl['Rho_kl_M']*GIRR_Diff_Mlt
GIRR_delta_kl.loc[((GIRR_delta_kl.RISK_FACTOR_TYPE_K=='basis') | (GIRR_delta_kl.RISK_FACTOR_TYPE_L=='basis')) 
                  & (GIRR_delta_kl.RISK_FACTOR_TYPE_K!=GIRR_delta_kl.RISK_FACTOR_TYPE_L)
                  ,'Rho_kl_M'] = GIRR_Cross_Mlt


# In[12]:


GIRR_delta_kl['Rho_kl_H'] = np.minimum(1, GIRR_delta_kl['Rho_kl_M']*High_Multipler)
GIRR_delta_kl['Rho_kl_L'] = np.maximum((Low_Multipler1*GIRR_delta_kl['Rho_kl_M']-1),(Low_Multipler2*GIRR_delta_kl['Rho_kl_M']))
GIRR_delta_kl['rslt_kl_M'] = GIRR_delta_kl['WEIGHTED_SENSITIVITY_K']*GIRR_delta_kl['WEIGHTED_SENSITIVITY_L']*GIRR_delta_kl['Rho_kl_M']
GIRR_delta_kl['rslt_kl_H'] = GIRR_delta_kl['WEIGHTED_SENSITIVITY_K']*GIRR_delta_kl['WEIGHTED_SENSITIVITY_L']*GIRR_delta_kl['Rho_kl_H']
GIRR_delta_kl['rslt_kl_L'] = GIRR_delta_kl['WEIGHTED_SENSITIVITY_K']*GIRR_delta_kl['WEIGHTED_SENSITIVITY_L']*GIRR_delta_kl['Rho_kl_L']


# In[13]:


GIRR_delta_kl.loc[(GIRR_delta_kl.RISK_FACTOR_ID_K==GIRR_delta_kl.RISK_FACTOR_ID_L)
                  &(GIRR_delta_kl.RISK_FACTOR_VERTEX_1_K==GIRR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=0
GIRR_delta_kl.loc[(GIRR_delta_kl.RISK_FACTOR_ID_K==GIRR_delta_kl.RISK_FACTOR_ID_L)
                  &(GIRR_delta_kl.RISK_FACTOR_VERTEX_1_K==GIRR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=0
GIRR_delta_kl.loc[(GIRR_delta_kl.RISK_FACTOR_ID_K==GIRR_delta_kl.RISK_FACTOR_ID_L)
                  &(GIRR_delta_kl.RISK_FACTOR_VERTEX_1_K==GIRR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=0

GIRR_delta_kl.loc[(GIRR_delta_kl.RISK_FACTOR_ID_K!=GIRR_delta_kl.RISK_FACTOR_ID_L)
                  |(GIRR_delta_kl.RISK_FACTOR_VERTEX_1_K!=GIRR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=GIRR_delta_kl['WEIGHTED_SENSITIVITY_L']*GIRR_delta_kl['Rho_kl_M']
GIRR_delta_kl.loc[(GIRR_delta_kl.RISK_FACTOR_ID_K!=GIRR_delta_kl.RISK_FACTOR_ID_L)
                  |(GIRR_delta_kl.RISK_FACTOR_VERTEX_1_K!=GIRR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=GIRR_delta_kl['WEIGHTED_SENSITIVITY_L']*GIRR_delta_kl['Rho_kl_H']
GIRR_delta_kl.loc[(GIRR_delta_kl.RISK_FACTOR_ID_K!=GIRR_delta_kl.RISK_FACTOR_ID_L)
                  |(GIRR_delta_kl.RISK_FACTOR_VERTEX_1_K!=GIRR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=GIRR_delta_kl['WEIGHTED_SENSITIVITY_L']*GIRR_delta_kl['Rho_kl_L']


# In[14]:


GIRR_delta_agg = GIRR_delta.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()


# In[15]:


GIRR_delta_bc = GIRR_delta_agg.rename(
                    {'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                ).merge(GIRR_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c','WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                        ,on=['RISK_FACTOR_CLASS'],how='left')
GIRR_delta_bc = GIRR_delta_bc.loc[(GIRR_delta_bc.Bucket_b!=GIRR_delta_bc.Bucket_c),:].reset_index(drop=True)


# In[16]:


GIRR_delta_bc['Gamma_bc_M']=0.5
GIRR_delta_bc['Gamma_bc_H'] = np.minimum(1, GIRR_delta_bc['Gamma_bc_M']*High_Multipler)
GIRR_delta_bc['Gamma_bc_L'] = np.maximum((Low_Multipler1*GIRR_delta_bc['Gamma_bc_M']-1),(Low_Multipler2*GIRR_delta_bc['Gamma_bc_M']))
GIRR_delta_bc['rslt_bc_M']=GIRR_delta_bc.WS_b*GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_M
GIRR_delta_bc['rslt_bc_H']=GIRR_delta_bc.WS_b*GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_H
GIRR_delta_bc['rslt_bc_L']=GIRR_delta_bc.WS_b*GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_L


# In[17]:


GIRR_delta_bc['gammac_M']=GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_M
GIRR_delta_bc['gammac_H']=GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_H
GIRR_delta_bc['gammac_L']=GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_L


# In[18]:


GIRR_delta_agg=GIRR_delta_agg.merge(
    GIRR_delta_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']],on=['RISK_FACTOR_BUCKET'],how='left'
).groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']
                                      ,dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()
GIRR_delta_agg['Sb_H']=GIRR_delta_agg['WEIGHTED_SENSITIVITY']
GIRR_delta_agg['Sb_L']=GIRR_delta_agg['WEIGHTED_SENSITIVITY']
GIRR_delta_agg['rslt_kl_M']=np.maximum(GIRR_delta_agg['rslt_kl_M'],0)
GIRR_delta_agg['rslt_kl_H']=np.maximum(GIRR_delta_agg['rslt_kl_H'],0)
GIRR_delta_agg['rslt_kl_L']=np.maximum(GIRR_delta_agg['rslt_kl_L'],0)
GIRR_delta_agg['Kb_M']=np.sqrt(GIRR_delta_agg['rslt_kl_M'])
GIRR_delta_agg['Kb_H']=np.sqrt(GIRR_delta_agg['rslt_kl_H'])
GIRR_delta_agg['Kb_L']=np.sqrt(GIRR_delta_agg['rslt_kl_L'])
GIRR_delta_agg = GIRR_delta_agg.rename({'WEIGHTED_SENSITIVITY':'Sb_M','rslt_kl_M':'Kb_M^2','rslt_kl_H':'Kb_H^2','rslt_kl_L':'Kb_L^2'},axis=1)


# In[19]:


GIRR_delta_agg['Sb*_M']=np.maximum(np.minimum(GIRR_delta_agg['Kb_M'],GIRR_delta_agg['Sb_M']),-GIRR_delta_agg['Kb_M'])
GIRR_delta_agg['Sb*_H']=np.maximum(np.minimum(GIRR_delta_agg['Kb_H'],GIRR_delta_agg['Sb_H']),-GIRR_delta_agg['Kb_H'])
GIRR_delta_agg['Sb*_L']=np.maximum(np.minimum(GIRR_delta_agg['Kb_L'],GIRR_delta_agg['Sb_L']),-GIRR_delta_agg['Kb_L'])


# In[20]:


GIRR_delta_bc=GIRR_delta_bc.merge(
    GIRR_delta_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
    ,left_on=['Bucket_b'],right_on=['RISK_FACTOR_BUCKET'],how='left')

GIRR_delta_bc=GIRR_delta_bc.merge(
    GIRR_delta_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
    ,left_on=['Bucket_c'],right_on=['RISK_FACTOR_BUCKET'],how='left')

GIRR_delta_bc=GIRR_delta_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)


# In[21]:


GIRR_delta_bc['rslt_bc*_M']=GIRR_delta_bc['Sb*_M']*GIRR_delta_bc['Sc*_M']*GIRR_delta_bc['Gamma_bc_M']
GIRR_delta_bc['rslt_bc*_H']=GIRR_delta_bc['Sb*_H']*GIRR_delta_bc['Sc*_H']*GIRR_delta_bc['Gamma_bc_H']
GIRR_delta_bc['rslt_bc*_L']=GIRR_delta_bc['Sb*_L']*GIRR_delta_bc['Sc*_L']*GIRR_delta_bc['Gamma_bc_L']


# In[22]:


girrd = pd.DataFrame([],columns=['GROUPING','RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=['0'])


# In[23]:


girrd_M_est=sum(GIRR_delta_agg['Kb_M^2'])+sum(GIRR_delta_bc['rslt_bc_M'])
girrd_M_1=np.sqrt(sum(GIRR_delta_agg['Kb_M^2'])+sum(GIRR_delta_bc['rslt_bc_M']))
girrd_M_2=np.sqrt(sum(GIRR_delta_agg['Kb_M^2'])+sum(GIRR_delta_bc['rslt_bc*_M']))


# In[24]:


girrd_H_est=sum(GIRR_delta_agg['Kb_H^2'])+sum(GIRR_delta_bc['rslt_bc_H'])
girrd_H_1=np.sqrt(sum(GIRR_delta_agg['Kb_H^2'])+sum(GIRR_delta_bc['rslt_bc_H']))
girrd_H_2=np.sqrt(sum(GIRR_delta_agg['Kb_H^2'])+sum(GIRR_delta_bc['rslt_bc*_H']))


# In[25]:


girrd_L_est=sum(GIRR_delta_agg['Kb_L^2'])+sum(GIRR_delta_bc['rslt_bc_L'])
girrd_L_1=np.sqrt(sum(GIRR_delta_agg['Kb_L^2'])+sum(GIRR_delta_bc['rslt_bc_L']))
girrd_L_2=np.sqrt(sum(GIRR_delta_agg['Kb_L^2'])+sum(GIRR_delta_bc['rslt_bc*_L']))


# In[26]:


girrd['RISK_FACTOR_CLASS']='GIRR'
girrd['SENS_TYPE']='DELTA'
girrd['NORMAL']=np.where(girrd_M_est>=0,girrd_M_1,girrd_M_2)
girrd['HIGH']=np.where(girrd_H_est>=0,girrd_H_1,girrd_H_2)
girrd['LOW']=np.where(girrd_L_est>=0,girrd_L_1,girrd_L_2)


# In[27]:


girrd_1=GIRR_delta[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]


# In[28]:


girrd_2=GIRR_delta_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                      ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()


# In[29]:


girrd_3=GIRR_delta_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                      ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()


# In[30]:


girrd_4=GIRR_delta_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]


# In[31]:


girrd_decomp=girrd_1.merge(girrd_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET']
                          ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET'],how='left')\
.merge(girrd_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
.merge(girrd_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
.merge(girrd,on=['RISK_FACTOR_CLASS'],how='left')


# In[32]:


girrd_decomp=girrd_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','Bucket_b','GROUPING','SENS_TYPE'],axis=1)


# In[33]:


girrd_decomp['M_est']=girrd_M_est
girrd_decomp['H_est']=girrd_H_est
girrd_decomp['L_est']=girrd_L_est


# In[34]:


#case 1
girrd_decomp.loc[(girrd_decomp['M_est']>=0)&(girrd_decomp['Kb_M']>0),'pder_M']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_M']+girrd_decomp['gammac_M'])/girrd_decomp['NORMAL']

girrd_decomp.loc[(girrd_decomp['H_est']>=0)&(girrd_decomp['Kb_H']>0),'pder_H']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_H']+girrd_decomp['gammac_H'])/girrd_decomp['HIGH']

girrd_decomp.loc[(girrd_decomp['L_est']>=0)&(girrd_decomp['Kb_L']>0),'pder_L']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_L']+girrd_decomp['gammac_L'])/girrd_decomp['LOW']


# In[35]:


#case 2
girrd_decomp.loc[(girrd_decomp['M_est']>=0)&(girrd_decomp['Kb_M']==0),'pder_M']=girrd_decomp['gammac_M']/girrd_decomp['NORMAL']

girrd_decomp.loc[(girrd_decomp['H_est']>=0)&(girrd_decomp['Kb_H']==0),'pder_H']=girrd_decomp['gammac_H']/girrd_decomp['HIGH']

girrd_decomp.loc[(girrd_decomp['L_est']>=0)&(girrd_decomp['Kb_L']==0),'pder_L']=girrd_decomp['gammac_L']/girrd_decomp['LOW']


# In[36]:


#case 3
girrd_decomp.loc[(girrd_decomp['M_est']<0)&(girrd_decomp['Kb_M']>0)&(girrd_decomp['Sb*_M']==girrd_decomp['Kb_M']),'pder_M']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_M'])*(1+1/girrd_decomp['Kb_M']*girrd_decomp['gammac_M']))/girrd_decomp['NORMAL']

girrd_decomp.loc[(girrd_decomp['H_est']<0)&(girrd_decomp['Kb_H']>0)&(girrd_decomp['Sb*_H']==girrd_decomp['Kb_H']),'pder_H']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_H'])*(1+1/girrd_decomp['Kb_H']*girrd_decomp['gammac_H']))/girrd_decomp['HIGH']

girrd_decomp.loc[(girrd_decomp['L_est']<0)&(girrd_decomp['Kb_L']>0)&(girrd_decomp['Sb*_L']==girrd_decomp['Kb_L']),'pder_L']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_L'])*(1+1/girrd_decomp['Kb_L']*girrd_decomp['gammac_L']))/girrd_decomp['LOW']


# In[37]:


#case 4
girrd_decomp.loc[(girrd_decomp['M_est']<0)&(girrd_decomp['Kb_M']>0)&(girrd_decomp['Sb*_M']+girrd_decomp['Kb_M']==0),'pder_M']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_M'])*(1-1/girrd_decomp['Kb_M']*girrd_decomp['gammac_M']))/girrd_decomp['NORMAL']

girrd_decomp.loc[(girrd_decomp['H_est']<0)&(girrd_decomp['Kb_H']>0)&(girrd_decomp['Sb*_H']+girrd_decomp['Kb_H']==0),'pder_H']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_H'])*(1-1/girrd_decomp['Kb_H']*girrd_decomp['gammac_H']))/girrd_decomp['HIGH']

girrd_decomp.loc[(girrd_decomp['L_est']<0)&(girrd_decomp['Kb_L']>0)&(girrd_decomp['Sb*_L']+girrd_decomp['Kb_L']==0),'pder_L']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_L'])*(1-1/girrd_decomp['Kb_L']*girrd_decomp['gammac_L']))/girrd_decomp['LOW']


# In[38]:


#case 5
girrd_decomp.loc[(girrd_decomp['M_est']<0)&(girrd_decomp['Kb_M']>0)&(abs(girrd_decomp['Sb*_M'])!=abs(girrd_decomp['Kb_M'])),'pder_M']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_M']+girrd_decomp['gammac_M'])/girrd_decomp['NORMAL']

girrd_decomp.loc[(girrd_decomp['H_est']<0)&(girrd_decomp['Kb_H']>0)&(abs(girrd_decomp['Sb*_H'])!=abs(girrd_decomp['Kb_H'])),'pder_H']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_H']+girrd_decomp['gammac_H'])/girrd_decomp['HIGH']

girrd_decomp.loc[(girrd_decomp['L_est']<0)&(girrd_decomp['Kb_L']>0)&(abs(girrd_decomp['Sb*_L'])!=abs(girrd_decomp['Kb_L'])),'pder_L']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_L']+girrd_decomp['gammac_L'])/girrd_decomp['LOW']


# In[39]:


#case 6
girrd_decomp.loc[(girrd_decomp['M_est']<0)&(girrd_decomp['Kb_M']==0),'pder_M']=0

girrd_decomp.loc[(girrd_decomp['H_est']<0)&(girrd_decomp['Kb_H']==0),'pder_H']=0

girrd_decomp.loc[(girrd_decomp['L_est']<0)&(girrd_decomp['Kb_L']==0),'pder_L']=0


# In[40]:


girrd_decomp=girrd_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]


# In[41]:


girrd_decomp_rslt=GIRR_RawData[(GIRR_RawData.SENSITIVITY_TYPE=='Delta')].merge(girrd_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'],how='left')


# In[42]:


#sum(girrd_decomp_rslt['WEIGHTED_SENSITIVITY']*girrd_decomp_rslt['pder_M'])


# In[43]:


#sum(girrd_decomp_rslt['WEIGHTED_SENSITIVITY']*girrd_decomp_rslt['pder_H'])


# In[44]:


#sum(girrd_decomp_rslt['WEIGHTED_SENSITIVITY']*girrd_decomp_rslt['pder_L'])


# In[45]:


#girrd


# In[ ]:





# In[46]:


GIRR_vega=GIRR_Position[(GIRR_Position['SENSITIVITY_TYPE']=='Vega')]


# In[47]:


GIRR_vega_kl = GIRR_vega.rename(
    {'RISK_FACTOR_ID':'RISK_FACTOR_ID_K'
     ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_K'
     ,'RISK_FACTOR_VERTEX_2':'RISK_FACTOR_VERTEX_2_K'
     ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_K'},axis=1
).merge(GIRR_vega[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2',
                   'RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
        .rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_L'
                 ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_L'
                 ,'RISK_FACTOR_VERTEX_2':'RISK_FACTOR_VERTEX_2_L'
                 ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_L'},axis=1)
        ,on=['RISK_FACTOR_BUCKET'],how='left')


# In[48]:


GIRR_vega_kl['Rho_kl_opt_mat_M'] = np.exp(
    -0.01*abs(
        GIRR_vega_kl['RISK_FACTOR_VERTEX_1_K']-GIRR_vega_kl['RISK_FACTOR_VERTEX_1_L']
    )/np.minimum(GIRR_vega_kl['RISK_FACTOR_VERTEX_1_K'],GIRR_vega_kl['RISK_FACTOR_VERTEX_1_L']))


# In[49]:


GIRR_vega_kl['Rho_kl_und_mat_M'] = np.exp(
    -0.01*abs(
        GIRR_vega_kl['RISK_FACTOR_VERTEX_2_K']-GIRR_vega_kl['RISK_FACTOR_VERTEX_2_L']
    )/np.minimum(GIRR_vega_kl['RISK_FACTOR_VERTEX_2_K'],GIRR_vega_kl['RISK_FACTOR_VERTEX_2_L']))


# In[50]:


GIRR_vega_kl['Rho_kl_M']=np.minimum((GIRR_vega_kl['Rho_kl_opt_mat_M']*GIRR_vega_kl['Rho_kl_und_mat_M']),1)
GIRR_vega_kl['rslt_kl_M']=GIRR_vega_kl['Rho_kl_M']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_K']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_L']
GIRR_vega_kl['Rho_kl_H']=np.minimum(1,High_Multipler*GIRR_vega_kl['Rho_kl_M'])
GIRR_vega_kl['rslt_kl_H']=GIRR_vega_kl['Rho_kl_H']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_K']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_L']
GIRR_vega_kl['Rho_kl_L']=np.maximum(Low_Multipler1*GIRR_vega_kl['Rho_kl_M']-1,Low_Multipler2*GIRR_vega_kl['Rho_kl_M'])
GIRR_vega_kl['rslt_kl_L']=GIRR_vega_kl['Rho_kl_L']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_K']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_L']


# In[51]:


GIRR_vega_kl.loc[(GIRR_vega_kl.RISK_FACTOR_ID_K==GIRR_vega_kl.RISK_FACTOR_ID_L)
                 &(GIRR_vega_kl.RISK_FACTOR_VERTEX_1_K==GIRR_vega_kl.RISK_FACTOR_VERTEX_1_L)
                 &(GIRR_vega_kl.RISK_FACTOR_VERTEX_2_K==GIRR_vega_kl.RISK_FACTOR_VERTEX_2_L),'rhol_M']=0
GIRR_vega_kl.loc[(GIRR_vega_kl.RISK_FACTOR_ID_K==GIRR_vega_kl.RISK_FACTOR_ID_L)
                 &(GIRR_vega_kl.RISK_FACTOR_VERTEX_1_K==GIRR_vega_kl.RISK_FACTOR_VERTEX_1_L)
                 &(GIRR_vega_kl.RISK_FACTOR_VERTEX_2_K==GIRR_vega_kl.RISK_FACTOR_VERTEX_2_L),'rhol_H']=0
GIRR_vega_kl.loc[(GIRR_vega_kl.RISK_FACTOR_ID_K==GIRR_vega_kl.RISK_FACTOR_ID_L)
                 &(GIRR_vega_kl.RISK_FACTOR_VERTEX_1_K==GIRR_vega_kl.RISK_FACTOR_VERTEX_1_L)
                 &(GIRR_vega_kl.RISK_FACTOR_VERTEX_2_K==GIRR_vega_kl.RISK_FACTOR_VERTEX_2_L),'rhol_L']=0

GIRR_vega_kl.loc[(GIRR_vega_kl.RISK_FACTOR_ID_K!=GIRR_vega_kl.RISK_FACTOR_ID_L)
                 |(GIRR_vega_kl.RISK_FACTOR_VERTEX_1_K!=GIRR_vega_kl.RISK_FACTOR_VERTEX_1_L)
                 |(GIRR_vega_kl.RISK_FACTOR_VERTEX_2_K!=GIRR_vega_kl.RISK_FACTOR_VERTEX_2_L),'rhol_M']=GIRR_vega_kl['WEIGHTED_SENSITIVITY_L']*GIRR_vega_kl['Rho_kl_M']
GIRR_vega_kl.loc[(GIRR_vega_kl.RISK_FACTOR_ID_K!=GIRR_vega_kl.RISK_FACTOR_ID_L)
                 |(GIRR_vega_kl.RISK_FACTOR_VERTEX_1_K!=GIRR_vega_kl.RISK_FACTOR_VERTEX_1_L)
                 |(GIRR_vega_kl.RISK_FACTOR_VERTEX_2_K!=GIRR_vega_kl.RISK_FACTOR_VERTEX_2_L),'rhol_H']=GIRR_vega_kl['WEIGHTED_SENSITIVITY_L']*GIRR_vega_kl['Rho_kl_H']
GIRR_vega_kl.loc[(GIRR_vega_kl.RISK_FACTOR_ID_K!=GIRR_vega_kl.RISK_FACTOR_ID_L)
                 |(GIRR_vega_kl.RISK_FACTOR_VERTEX_1_K!=GIRR_vega_kl.RISK_FACTOR_VERTEX_1_L)
                 |(GIRR_vega_kl.RISK_FACTOR_VERTEX_2_K!=GIRR_vega_kl.RISK_FACTOR_VERTEX_2_L),'rhol_L']=GIRR_vega_kl['WEIGHTED_SENSITIVITY_L']*GIRR_vega_kl['Rho_kl_L']


# In[52]:


GIRR_vega_agg = GIRR_vega.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()


# In[53]:


GIRR_vega_bc = GIRR_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
               ).merge(GIRR_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c','WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                       ,on='RISK_FACTOR_CLASS',how='left')


# In[54]:


GIRR_vega_bc.loc[(GIRR_vega_bc['Bucket_b']==GIRR_vega_bc['Bucket_c']),['Gamma_bc_M']]=1
GIRR_vega_bc.loc[(GIRR_vega_bc['Bucket_b']!=GIRR_vega_bc['Bucket_c']),['Gamma_bc_M']]=0.5
GIRR_vega_bc['Gamma_bc_H'] = np.minimum(1, GIRR_vega_bc['Gamma_bc_M']*High_Multipler)
GIRR_vega_bc['Gamma_bc_L'] = np.maximum((Low_Multipler1*GIRR_vega_bc['Gamma_bc_M']-1),(Low_Multipler2*GIRR_vega_bc['Gamma_bc_M']))
GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_M']==1,'rslt_bc_M']=0
GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_M']!=1,'rslt_bc_M']=GIRR_vega_bc.WS_b*GIRR_vega_bc.WS_c*GIRR_vega_bc.Gamma_bc_M
GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_H']==1,'rslt_bc_H']=0
GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_H']!=1,'rslt_bc_H']=GIRR_vega_bc.WS_b*GIRR_vega_bc.WS_c*GIRR_vega_bc.Gamma_bc_H
GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_L']==1,'rslt_bc_L']=0
GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_L']!=1,'rslt_bc_L']=GIRR_vega_bc.WS_b*GIRR_vega_bc.WS_c*GIRR_vega_bc.Gamma_bc_L


# In[55]:


GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']==GIRR_vega_bc['Bucket_c'],'gammac_M']=0
GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']!=GIRR_vega_bc['Bucket_c'],'gammac_M']=GIRR_vega_bc.WS_c*GIRR_vega_bc.Gamma_bc_M
GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']==GIRR_vega_bc['Bucket_c'],'gammac_H']=0
GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']!=GIRR_vega_bc['Bucket_c'],'gammac_H']=GIRR_vega_bc.WS_c*GIRR_vega_bc.Gamma_bc_H
GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']==GIRR_vega_bc['Bucket_c'],'gammac_L']=0
GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']!=GIRR_vega_bc['Bucket_c'],'gammac_L']=GIRR_vega_bc.WS_c*GIRR_vega_bc.Gamma_bc_L


# In[56]:


GIRR_vega_agg=GIRR_vega_agg.merge(
    GIRR_vega_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']],on='RISK_FACTOR_BUCKET',how='left'
).groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']
                                      ,dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()
GIRR_vega_agg['Sb_H']=GIRR_vega_agg['WEIGHTED_SENSITIVITY']
GIRR_vega_agg['Sb_L']=GIRR_vega_agg['WEIGHTED_SENSITIVITY']
GIRR_vega_agg['Kb_M']=np.sqrt(GIRR_vega_agg['rslt_kl_M'])
GIRR_vega_agg['Kb_H']=np.sqrt(GIRR_vega_agg['rslt_kl_H'])
GIRR_vega_agg['Kb_L']=np.sqrt(GIRR_vega_agg['rslt_kl_L'])
GIRR_vega_agg = GIRR_vega_agg.rename({'WEIGHTED_SENSITIVITY':'Sb_M','rslt_kl_M':'Kb_M^2','rslt_kl_H':'Kb_H^2','rslt_kl_L':'Kb_L^2'},axis=1)


# In[57]:


GIRR_vega_agg['Sb*_M']=np.maximum(np.minimum(GIRR_vega_agg['Kb_M'],GIRR_vega_agg['Sb_M']),-GIRR_vega_agg['Kb_M'])
GIRR_vega_agg['Sb*_H']=np.maximum(np.minimum(GIRR_vega_agg['Kb_H'],GIRR_vega_agg['Sb_H']),-GIRR_vega_agg['Kb_H'])
GIRR_vega_agg['Sb*_L']=np.maximum(np.minimum(GIRR_vega_agg['Kb_L'],GIRR_vega_agg['Sb_L']),-GIRR_vega_agg['Kb_L'])

GIRR_vega_bc=GIRR_vega_bc.merge(
    GIRR_vega_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
    ,left_on='Bucket_b',right_on='RISK_FACTOR_BUCKET',how='left')

GIRR_vega_bc=GIRR_vega_bc.merge(
    GIRR_vega_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
    ,left_on='Bucket_c',right_on='RISK_FACTOR_BUCKET',how='left')

GIRR_vega_bc=GIRR_vega_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)

GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_M']==1,'rslt_bc*_M']=0
GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_M']!=1,'rslt_bc*_M']=GIRR_vega_bc['Sb*_M']*GIRR_vega_bc['Sc*_M']*GIRR_vega_bc['Gamma_bc_M']
GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_H']==1,'rslt_bc*_H']=0
GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_H']!=1,'rslt_bc*_H']=GIRR_vega_bc['Sb*_H']*GIRR_vega_bc['Sc*_H']*GIRR_vega_bc['Gamma_bc_H']
GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_L']==1,'rslt_bc*_L']=0
GIRR_vega_bc.loc[GIRR_vega_bc['Gamma_bc_L']!=1,'rslt_bc*_L']=GIRR_vega_bc['Sb*_L']*GIRR_vega_bc['Sc*_L']*GIRR_vega_bc['Gamma_bc_L']


# In[58]:


girrv=pd.DataFrame([],columns=['GROUPING','RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])


# In[59]:


girrv_M_est=sum(GIRR_vega_agg['Kb_M^2'])+sum(GIRR_vega_bc['rslt_bc_M'])
girrv_M_1=np.sqrt(sum(GIRR_vega_agg['Kb_M^2'])+sum(GIRR_vega_bc['rslt_bc_M']))
girrv_M_2=np.sqrt(sum(GIRR_vega_agg['Kb_M^2'])+sum(GIRR_vega_bc['rslt_bc*_M']))


# In[60]:


girrv_H_est=sum(GIRR_vega_agg['Kb_H^2'])+sum(GIRR_vega_bc['rslt_bc_H'])
girrv_H_1=np.sqrt(sum(GIRR_vega_agg['Kb_H^2'])+sum(GIRR_vega_bc['rslt_bc_H']))
girrv_H_2=np.sqrt(sum(GIRR_vega_agg['Kb_H^2'])+sum(GIRR_vega_bc['rslt_bc*_H']))


# In[61]:


girrv_L_est=sum(GIRR_vega_agg['Kb_L^2'])+sum(GIRR_vega_bc['rslt_bc_L'])
girrv_L_1=np.sqrt(sum(GIRR_vega_agg['Kb_L^2'])+sum(GIRR_vega_bc['rslt_bc_L']))
girrv_L_2=np.sqrt(sum(GIRR_vega_agg['Kb_L^2'])+sum(GIRR_vega_bc['rslt_bc*_L']))


# In[62]:


girrv['RISK_FACTOR_CLASS']='GIRR'
girrv['SENS_TYPE']='VEGA'
girrv['NORMAL']=np.where(girrv_M_est>=0,girrv_M_1,girrv_M_2)
girrv['HIGH']=np.where(girrv_H_est>=0,girrv_H_1,girrv_H_2)
girrv['LOW']=np.where(girrv_L_est>=0,girrv_L_1,girrv_L_2)


# In[63]:


girrv_1=GIRR_vega[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]


# In[64]:


girrv_2=GIRR_vega_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_VERTEX_2_K','RISK_FACTOR_BUCKET']
                             ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()


# In[65]:


girrv_3=GIRR_vega_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                             ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()


# In[66]:


girrv_4=GIRR_vega_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]


# In[67]:


girrv_decomp=girrv_1.merge(girrv_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_BUCKET']
                           ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_VERTEX_2_K','RISK_FACTOR_BUCKET']
                           ,how='left')\
.merge(girrv_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
.merge(girrv_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
.merge(girrv,on=['RISK_FACTOR_CLASS'],how='left')


# In[68]:


girrv_decomp=girrv_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_VERTEX_2_K','Bucket_b','GROUPING','SENS_TYPE'],axis=1)


# In[69]:


girrv_decomp['M_est']=girrv_M_est
girrv_decomp['H_est']=girrv_H_est
girrv_decomp['L_est']=girrv_L_est


# In[70]:


#case 1
girrv_decomp.loc[(girrv_decomp['M_est']>=0)&(girrv_decomp['Kb_M']>0),'pder_M']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_M']+girrv_decomp['gammac_M'])/girrv_decomp['NORMAL']

girrv_decomp.loc[(girrv_decomp['H_est']>=0)&(girrv_decomp['Kb_H']>0),'pder_H']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_H']+girrv_decomp['gammac_H'])/girrv_decomp['HIGH']

girrv_decomp.loc[(girrv_decomp['L_est']>=0)&(girrv_decomp['Kb_L']>0),'pder_L']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_L']+girrv_decomp['gammac_L'])/girrv_decomp['LOW']


# In[71]:


#case 2
girrv_decomp.loc[(girrv_decomp['M_est']>=0)&(girrv_decomp['Kb_M']==0),'pder_M']=girrv_decomp['gammac_M']/girrv_decomp['NORMAL']

girrv_decomp.loc[(girrv_decomp['H_est']>=0)&(girrv_decomp['Kb_H']==0),'pder_H']=girrv_decomp['gammac_H']/girrv_decomp['HIGH']

girrv_decomp.loc[(girrv_decomp['L_est']>=0)&(girrv_decomp['Kb_L']==0),'pder_L']=girrv_decomp['gammac_L']/girrv_decomp['LOW']


# In[72]:


#case 3
girrv_decomp.loc[(girrv_decomp['M_est']<0)&(girrv_decomp['Kb_M']>0)&(girrv_decomp['Sb*_M']==girrv_decomp['Kb_M']),'pder_M']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_M'])*(1+1/girrv_decomp['Kb_M']*girrv_decomp['gammac_M']))/girrv_decomp['NORMAL']

girrv_decomp.loc[(girrv_decomp['H_est']<0)&(girrv_decomp['Kb_H']>0)&(girrv_decomp['Sb*_H']==girrv_decomp['Kb_H']),'pder_H']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_H'])*(1+1/girrv_decomp['Kb_H']*girrv_decomp['gammac_H']))/girrv_decomp['HIGH']

girrv_decomp.loc[(girrv_decomp['L_est']<0)&(girrv_decomp['Kb_L']>0)&(girrv_decomp['Sb*_L']==girrv_decomp['Kb_L']),'pder_L']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_L'])*(1+1/girrv_decomp['Kb_L']*girrv_decomp['gammac_L']))/girrv_decomp['LOW']


# In[73]:


#case 4
girrv_decomp.loc[(girrv_decomp['M_est']<0)&(girrv_decomp['Kb_M']>0)&(girrv_decomp['Sb*_M']+girrv_decomp['Kb_M']==0),'pder_M']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_M'])*(1-1/girrv_decomp['Kb_M']*girrv_decomp['gammac_M']))/girrv_decomp['NORMAL']

girrv_decomp.loc[(girrv_decomp['H_est']<0)&(girrv_decomp['Kb_H']>0)&(girrv_decomp['Sb*_H']+girrv_decomp['Kb_H']==0),'pder_H']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_H'])*(1-1/girrv_decomp['Kb_H']*girrv_decomp['gammac_H']))/girrv_decomp['HIGH']

girrv_decomp.loc[(girrv_decomp['L_est']<0)&(girrv_decomp['Kb_L']>0)&(girrv_decomp['Sb*_L']+girrv_decomp['Kb_L']==0),'pder_L']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_L'])*(1-1/girrv_decomp['Kb_L']*girrv_decomp['gammac_L']))/girrv_decomp['LOW']


# In[74]:


#case 5
girrv_decomp.loc[(girrv_decomp['M_est']<0)&(girrv_decomp['Kb_M']>0)&(abs(girrv_decomp['Sb*_M'])!=abs(girrv_decomp['Kb_M'])),'pder_M']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_M']+girrv_decomp['gammac_M'])/girrv_decomp['NORMAL']

girrv_decomp.loc[(girrv_decomp['H_est']<0)&(girrv_decomp['Kb_H']>0)&(abs(girrv_decomp['Sb*_H'])!=abs(girrv_decomp['Kb_H'])),'pder_H']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_H']+girrv_decomp['gammac_H'])/girrv_decomp['HIGH']

girrv_decomp.loc[(girrv_decomp['L_est']<0)&(girrv_decomp['Kb_L']>0)&(abs(girrv_decomp['Sb*_L'])!=abs(girrv_decomp['Kb_L'])),'pder_L']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_L']+girrv_decomp['gammac_L'])/girrv_decomp['LOW']


# In[75]:


#case 6
girrv_decomp.loc[(girrv_decomp['M_est']<0)&(girrv_decomp['Kb_M']==0),'pder_M']=0

girrv_decomp.loc[(girrv_decomp['H_est']<0)&(girrv_decomp['Kb_H']==0),'pder_H']=0

girrv_decomp.loc[(girrv_decomp['L_est']<0)&(girrv_decomp['Kb_L']==0),'pder_L']=0


# In[76]:


girrv_decomp=girrv_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]


# In[77]:


girrv_decomp_rslt=GIRR_RawData[(GIRR_RawData.SENSITIVITY_TYPE=='Vega')].merge(girrv_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_BUCKET'],how='left')


# In[78]:


girrv_decomp_rslt=girrv_decomp_rslt.fillna({'pder_M':0,'pder_H':0,'pder_L':0})


# In[ ]:





# In[ ]:





# In[ ]:





# In[79]:


GIRR_curvature = GIRR_Position.query('SENSITIVITY_TYPE=="Curvature Up"|SENSITIVITY_TYPE=="Curvature Down"')


# In[80]:


GIRR_curvature = GIRR_curvature.assign(max_0_square=np.square(np.maximum(GIRR_curvature['WEIGHTED_SENSITIVITY'],0)))


# In[81]:


GIRR_curvature_agg = GIRR_curvature.groupby(
    ['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],dropna=False
).agg({'WEIGHTED_SENSITIVITY':'sum','max_0_square':'sum'}).reset_index()


# In[82]:


GIRR_curvature_agg['max_0_k']=np.sqrt(GIRR_curvature_agg['max_0_square'])


# In[83]:


GIRR_curvature_agg=GIRR_curvature_agg.pivot(index=('RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET')
                         ,columns='SENSITIVITY_TYPE')


# In[84]:


GIRR_curvature_agg.columns=['/'.join(i) for i in GIRR_curvature_agg.columns]
GIRR_curvature_agg=GIRR_curvature_agg.reset_index()


# In[85]:


GIRR_curvature_agg['Kb+_M']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Up'])))
GIRR_curvature_agg['Kb-_M']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Down'])))
GIRR_curvature_agg['Kb_M']=np.maximum(GIRR_curvature_agg['Kb+_M'],GIRR_curvature_agg['Kb-_M'])
GIRR_curvature_agg['Kb_M^2']=np.square(GIRR_curvature_agg['Kb_M'])
GIRR_curvature_agg['Sb_M']=np.select([(GIRR_curvature_agg['Kb_M'] == GIRR_curvature_agg['Kb+_M']),
                                      (GIRR_curvature_agg['Kb_M'] != GIRR_curvature_agg['Kb+_M'])],
                                     [(GIRR_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Up']),
                                      (GIRR_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Down'])])


# In[86]:


GIRR_curvature_agg['Kb+_H']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Up'])))
GIRR_curvature_agg['Kb-_H']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Down'])))
GIRR_curvature_agg['Kb_H']=np.maximum(GIRR_curvature_agg['Kb+_H'],GIRR_curvature_agg['Kb-_H'])
GIRR_curvature_agg['Kb_H^2']=np.square(GIRR_curvature_agg['Kb_H'])
GIRR_curvature_agg['Sb_H']=np.select([(GIRR_curvature_agg['Kb_H'] == GIRR_curvature_agg['Kb+_H']),
                                      (GIRR_curvature_agg['Kb_H'] != GIRR_curvature_agg['Kb+_H'])],
                                     [(GIRR_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Up']),
                                      (GIRR_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Down'])])


# In[87]:


GIRR_curvature_agg['Kb+_L']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Up'])))
GIRR_curvature_agg['Kb-_L']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Down'])))
GIRR_curvature_agg['Kb_L']=np.maximum(GIRR_curvature_agg['Kb+_L'],GIRR_curvature_agg['Kb-_L'])
GIRR_curvature_agg['Kb_L^2']=np.square(GIRR_curvature_agg['Kb_L'])
GIRR_curvature_agg['Sb_L']=np.select([(GIRR_curvature_agg['Kb_L'] == GIRR_curvature_agg['Kb+_L']),
                                      (GIRR_curvature_agg['Kb_L'] != GIRR_curvature_agg['Kb+_L'])],
                                     [(GIRR_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Up']),
                                      (GIRR_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Down'])])


# In[88]:


GIRR_curvature_agg['max']=np.select([(GIRR_curvature_agg['Kb_M'] == GIRR_curvature_agg['Kb+_M']),
                                     (GIRR_curvature_agg['Kb_M'] != GIRR_curvature_agg['Kb+_M'])],
                                    [(GIRR_curvature_agg['max_0_k/Curvature Up']),
                                     (GIRR_curvature_agg['max_0_k/Curvature Down'])])


# In[89]:


GIRR_curvature_agg['sign']=np.select([(GIRR_curvature_agg['Kb_M'] == GIRR_curvature_agg['Kb+_M']),
                                      (GIRR_curvature_agg['Kb_M'] != GIRR_curvature_agg['Kb+_M'])],
                                     ['Curvature Up','Curvature Down'])


# In[90]:


GIRR_curvature_bc=GIRR_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Sb_M']]
GIRR_curvature_bc=GIRR_curvature_bc.rename(
    {'Sb_M':'Sb','RISK_FACTOR_BUCKET':'Bucket_b'},axis=1
).merge(GIRR_curvature_bc.rename(
    {'Sb_M':'Sc','RISK_FACTOR_BUCKET':'Bucket_c'},axis=1
),on=['RISK_FACTOR_CLASS'],how='left')
GIRR_curvature_bc=GIRR_curvature_bc[(GIRR_curvature_bc['Bucket_b']!=GIRR_curvature_bc['Bucket_c'])]


# In[91]:


GIRR_curvature_bc.loc[(GIRR_curvature_bc['Sb']<0) & (GIRR_curvature_bc['Sc']<0),'Psi']=0
GIRR_curvature_bc.loc[(GIRR_curvature_bc['Sb']>=0) | (GIRR_curvature_bc['Sc']>=0),'Psi']=1
GIRR_curvature_bc['Gamma_bc']=GIRR_Gamma
GIRR_curvature_bc['Gamma_bc_M']=np.square(GIRR_curvature_bc['Gamma_bc'])


# In[92]:


GIRR_curvature_bc['rslt_bc_M']=GIRR_curvature_bc['Gamma_bc_M']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sb']*GIRR_curvature_bc['Sc']
GIRR_curvature_bc['Gamma_bc_H']=np.square(np.minimum(1,GIRR_curvature_bc['Gamma_bc']*High_Multipler))
GIRR_curvature_bc['Gamma_bc_L']=np.square(np.maximum((Low_Multipler1*GIRR_curvature_bc['Gamma_bc']-1),(Low_Multipler2*GIRR_curvature_bc['Gamma_bc'])))
GIRR_curvature_bc['rslt_bc_H']=GIRR_curvature_bc['Gamma_bc_H']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sb']*GIRR_curvature_bc['Sc']
GIRR_curvature_bc['rslt_bc_L']=GIRR_curvature_bc['Gamma_bc_L']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sb']*GIRR_curvature_bc['Sc']


# In[93]:


GIRR_curvature_bc['gammac_M']=GIRR_curvature_bc['Gamma_bc_M']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sc']
GIRR_curvature_bc['gammac_H']=GIRR_curvature_bc['Gamma_bc_H']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sc']
GIRR_curvature_bc['gammac_L']=GIRR_curvature_bc['Gamma_bc_L']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sc']


# In[94]:


girrc_M_est=sum(GIRR_curvature_agg['Kb_M^2'])+sum(GIRR_curvature_bc['rslt_bc_M'])
girrc_H_est=sum(GIRR_curvature_agg['Kb_H^2'])+sum(GIRR_curvature_bc['rslt_bc_H'])
girrc_L_est=sum(GIRR_curvature_agg['Kb_L^2'])+sum(GIRR_curvature_bc['rslt_bc_L'])


# In[95]:


girrc_M = np.sqrt(np.maximum(0,sum(GIRR_curvature_agg['Kb_M^2'])+sum(GIRR_curvature_bc['rslt_bc_M'])))
girrc_H = np.sqrt(np.maximum(0,sum(GIRR_curvature_agg['Kb_H^2'])+sum(GIRR_curvature_bc['rslt_bc_H'])))
girrc_L = np.sqrt(np.maximum(0,sum(GIRR_curvature_agg['Kb_L^2'])+sum(GIRR_curvature_bc['rslt_bc_L'])))


# In[96]:


girrc=pd.DataFrame([],columns=['GROUPING','RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])


# In[97]:


girrc['RISK_FACTOR_CLASS']='GIRR'
girrc['SENS_TYPE']='CURVATURE'
girrc['NORMAL']=girrc_M
girrc['HIGH']=girrc_H
girrc['LOW']=girrc_L


# In[98]:


girrc_1=GIRR_curvature[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]


# In[99]:


girrc_3=GIRR_curvature_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                                  ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()


# In[100]:


girrc_4=GIRR_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','max','sign']]


# In[101]:


girrc_decomp=girrc_1.merge(girrc_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET']
                           ,right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
.merge(girrc_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
.merge(girrc,on=['RISK_FACTOR_CLASS'],how='left')


# In[102]:


girrc_decomp=girrc_decomp.drop(['Bucket_b','GROUPING','SENS_TYPE'],axis=1)


# In[103]:


girrc_decomp['M_est']=girrc_M_est
girrc_decomp['H_est']=girrc_H_est
girrc_decomp['L_est']=girrc_L_est


# In[104]:


girrc_decomp=girrc_decomp[(girrc_decomp.SENSITIVITY_TYPE==girrc_decomp.sign)]


# In[105]:


#case 1/2
girrc_decomp.loc[(girrc_decomp['M_est']>=0),'pder_M']=(girrc_decomp['max']+girrc_decomp['gammac_M'])/girrc_decomp['NORMAL']

girrc_decomp.loc[(girrc_decomp['H_est']>=0),'pder_H']=(girrc_decomp['max']+girrc_decomp['gammac_H'])/girrc_decomp['HIGH']

girrc_decomp.loc[(girrc_decomp['L_est']>=0),'pder_L']=(girrc_decomp['max']+girrc_decomp['gammac_L'])/girrc_decomp['LOW']


# In[106]:


#case 3 
girrc_decomp.loc[(girrc_decomp['M_est']<0),'pder_M']=0
girrc_decomp.loc[(girrc_decomp['H_est']<0),'pder_H']=0
girrc_decomp.loc[(girrc_decomp['L_est']<0),'pder_L']=0


# In[107]:


girrc_decomp=girrc_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','pder_M','pder_H','pder_L']]


# In[108]:


girrc_decomp_rslt=GIRR_RawData.query('SENSITIVITY_TYPE=="Curvature Up"|SENSITIVITY_TYPE=="Curvature Down"').merge(girrc_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],how='right')


# In[109]:


#sum(girrc_decomp_rslt['WEIGHTED_SENSITIVITY']*girrc_decomp_rslt['pder_M'])


# In[110]:


#sum(girrc_decomp_rslt['WEIGHTED_SENSITIVITY']*girrc_decomp_rslt['pder_H'])


# In[111]:


#sum(girrc_decomp_rslt['WEIGHTED_SENSITIVITY']*girrc_decomp_rslt['pder_L'])


# In[112]:


#girrc


# In[ ]:





# In[ ]:





# In[ ]:





# In[113]:


#CSR(non-sec)


# In[190]:


CSR_Weights = params.parse('CSR_Weights')
CSR_Rho_Name = 0.35
CSR_Rho_Tenor = 0.65
CSR_Rho_Basis = 0.999
CSR_Gamma = params.parse('CSR_Gamma')
CSR_Gamma['Gamma_bc'] = CSR_Gamma['Gamma_bc_Rating']*CSR_Gamma['Gamma_bc_Sector']
CSR_LH = 120
CSR_vega_rw = 1


# In[191]:


CSR_Gamma['Bucket_b']=CSR_Gamma['Bucket_b'].astype(str)
CSR_Gamma['Bucket_c']=CSR_Gamma['Bucket_c'].astype(str)


# In[192]:


CSR_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='CSR (non-sec)')]

CSR_Position=CSR_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_TYPE'
                          ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SEC_ISSUER'
                          ,'SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]

CSR_Position=CSR_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_TYPE'
                                   ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SEC_ISSUER'
                                   ,'SENSITIVITY_TYPE'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()

CSR_delta=CSR_Position[(CSR_Position['SENSITIVITY_TYPE']=='Delta')]
CSR_delta['abs_WS']=abs(CSR_delta['WEIGHTED_SENSITIVITY'])


# In[193]:


#CSR_RawData=Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='CSR (non-sec)')]
#CSR_Position=CSR_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE'
#                          ,'SEC_ISSUER','SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]
#CSR_delta=CSR_Position[(CSR_Position['SENSITIVITY_TYPE']=='Delta')]
#CSR_delta['abs_WS']=abs(CSR_Position['WEIGHTED_SENSITIVITY'])
#CSR_delta=CSR_delta.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE'
#                              ,'SEC_ISSUER','SENSITIVITY_TYPE']
#                            ,dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum','abs_WS':'sum'}).reset_index()


# In[194]:


CSR_delta_kl=CSR_delta.rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_K'
                               ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_K'
                               ,'RISK_FACTOR_TYPE':'RISK_FACTOR_TYPE_K'
                               ,'SEC_ISSUER':'ISSUER_K'
                               ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_K'},axis=1
                             ).merge(CSR_delta[['RISK_FACTOR_ID','RISK_FACTOR_BUCKET'
                                                ,'RISK_FACTOR_VERTEX_1'
                                                ,'RISK_FACTOR_TYPE'
                                                ,'SEC_ISSUER'
                                                ,'WEIGHTED_SENSITIVITY']]
                                     .rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_L'
                                              ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_L'
                                              ,'RISK_FACTOR_TYPE':'RISK_FACTOR_TYPE_L'
                                              ,'SEC_ISSUER':'ISSUER_L'
                                              ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_L'},axis=1)
                                     ,on=['RISK_FACTOR_BUCKET'],how='left')


# In[195]:


CSR_delta_kl.loc[CSR_delta_kl['ISSUER_K'] == CSR_delta_kl['ISSUER_L'], 'Rho_name'] = 1
CSR_delta_kl.loc[CSR_delta_kl['ISSUER_K'] != CSR_delta_kl['ISSUER_L'], 'Rho_name'] = CSR_Rho_Name
CSR_delta_kl.loc[CSR_delta_kl['RISK_FACTOR_VERTEX_1_K'] == CSR_delta_kl['RISK_FACTOR_VERTEX_1_L'], 'Rho_tenor'] = 1
CSR_delta_kl.loc[CSR_delta_kl['RISK_FACTOR_VERTEX_1_K'] != CSR_delta_kl['RISK_FACTOR_VERTEX_1_L'], 'Rho_tenor'] = CSR_Rho_Tenor
CSR_delta_kl.loc[CSR_delta_kl['RISK_FACTOR_TYPE_K'] == CSR_delta_kl['RISK_FACTOR_TYPE_L'], 'Rho_basis'] = 1
CSR_delta_kl.loc[CSR_delta_kl['RISK_FACTOR_TYPE_K'] != CSR_delta_kl['RISK_FACTOR_TYPE_L'], 'Rho_basis'] = CSR_Rho_Basis


# In[196]:


CSR_delta_kl['Rho_kl_M'] = CSR_delta_kl['Rho_name']*CSR_delta_kl['Rho_tenor']*CSR_delta_kl['Rho_basis']
CSR_delta_kl['Rho_kl_H'] = np.minimum(1,High_Multipler*CSR_delta_kl['Rho_kl_M'])
CSR_delta_kl['Rho_kl_L'] = np.maximum(Low_Multipler1*CSR_delta_kl['Rho_kl_M']-1,Low_Multipler2*CSR_delta_kl['Rho_kl_M'])


# In[197]:


#CSR_delta_kl['rslt_kl_M']=np.where(pd.to_numeric(CSR_delta_kl.RISK_FACTOR_BUCKET)==16
#                                   ,abs(CSR_delta_kl['WEIGHTED_SENSITIVITY_K'])
#                                   ,CSR_delta_kl['WEIGHTED_SENSITIVITY_K']*CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_M'])
#CSR_delta_kl['rslt_kl_H']=np.where(pd.to_numeric(CSR_delta_kl.RISK_FACTOR_BUCKET)==16
#                                   ,abs(CSR_delta_kl['WEIGHTED_SENSITIVITY_K'])
#                                   ,CSR_delta_kl['WEIGHTED_SENSITIVITY_K']*CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_H'])
#CSR_delta_kl['rslt_kl_L']=np.where(pd.to_numeric(CSR_delta_kl.RISK_FACTOR_BUCKET)==16
#                                   ,abs(CSR_delta_kl['WEIGHTED_SENSITIVITY_K'])
#                                   ,CSR_delta_kl['WEIGHTED_SENSITIVITY_K']*CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_L'])


# In[198]:


CSR_delta_kl['rslt_kl_M']=CSR_delta_kl['WEIGHTED_SENSITIVITY_K']*CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_M']
CSR_delta_kl['rslt_kl_H']=CSR_delta_kl['WEIGHTED_SENSITIVITY_K']*CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_H']
CSR_delta_kl['rslt_kl_L']=CSR_delta_kl['WEIGHTED_SENSITIVITY_K']*CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_L']


# In[199]:


CSR_delta_kl.loc[(CSR_delta_kl.RISK_FACTOR_ID_K==CSR_delta_kl.RISK_FACTOR_ID_L)
                  &(CSR_delta_kl.RISK_FACTOR_VERTEX_1_K==CSR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=0
CSR_delta_kl.loc[(CSR_delta_kl.RISK_FACTOR_ID_K==CSR_delta_kl.RISK_FACTOR_ID_L)
                  &(CSR_delta_kl.RISK_FACTOR_VERTEX_1_K==CSR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=0
CSR_delta_kl.loc[(CSR_delta_kl.RISK_FACTOR_ID_K==CSR_delta_kl.RISK_FACTOR_ID_L)
                  &(CSR_delta_kl.RISK_FACTOR_VERTEX_1_K==CSR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=0

CSR_delta_kl.loc[(CSR_delta_kl.RISK_FACTOR_ID_K!=CSR_delta_kl.RISK_FACTOR_ID_L)
                  |(CSR_delta_kl.RISK_FACTOR_VERTEX_1_K!=CSR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_M']
CSR_delta_kl.loc[(CSR_delta_kl.RISK_FACTOR_ID_K!=CSR_delta_kl.RISK_FACTOR_ID_L)
                  |(CSR_delta_kl.RISK_FACTOR_VERTEX_1_K!=CSR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_H']
CSR_delta_kl.loc[(CSR_delta_kl.RISK_FACTOR_ID_K!=CSR_delta_kl.RISK_FACTOR_ID_L)
                  |(CSR_delta_kl.RISK_FACTOR_VERTEX_1_K!=CSR_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_L']


# In[200]:


CSR_delta_agg = CSR_delta.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET']
                                  ,dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum','abs_WS':'sum'}).reset_index()


# In[201]:


CSR_delta_bc = CSR_delta_agg.rename(
                    {'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b','abs_WS':'abs_WS_b'},axis=1
                ).merge(CSR_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c','WEIGHTED_SENSITIVITY':'WS_c','abs_WS':'abs_WS_c'},axis=1)
                        ,on=['RISK_FACTOR_CLASS'],how='left')


# In[202]:


CSR_delta_bc = CSR_delta_bc.loc[(CSR_delta_bc.Bucket_b!=CSR_delta_bc.Bucket_c),:].reset_index(drop=True)


# In[203]:


CSR_delta_bc = CSR_delta_bc.merge(CSR_Gamma,on=['Bucket_b','Bucket_c'],how='left').rename({'Gamma_bc':'Gamma_bc_M'},axis=1)
CSR_delta_bc['Gamma_bc_H'] = np.minimum(1, CSR_delta_bc['Gamma_bc_M']*High_Multipler)
CSR_delta_bc['Gamma_bc_L'] = np.maximum((Low_Multipler1*CSR_delta_bc['Gamma_bc_M']-1),(Low_Multipler2*CSR_delta_bc['Gamma_bc_M']))


# In[204]:


CSR_delta_bc['rslt_bc_M']=CSR_delta_bc.WS_b*CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_M
CSR_delta_bc['rslt_bc_H']=CSR_delta_bc.WS_b*CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_H
CSR_delta_bc['rslt_bc_L']=CSR_delta_bc.WS_b*CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_L


# In[205]:


CSR_delta_bc['gammac_M']=CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_M
CSR_delta_bc['gammac_H']=CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_H
CSR_delta_bc['gammac_L']=CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_L


# In[206]:


CSR_delta_agg=CSR_delta_agg.merge(
    CSR_delta_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']],on=['RISK_FACTOR_BUCKET'],how='left'
).groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY','abs_WS']
                                      ,dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()


# In[207]:


CSR_delta_agg['Sb_H']=CSR_delta_agg['WEIGHTED_SENSITIVITY']
CSR_delta_agg['Sb_L']=CSR_delta_agg['WEIGHTED_SENSITIVITY']
CSR_delta_agg['Kb_M']=np.where(pd.to_numeric(CSR_delta_agg['RISK_FACTOR_BUCKET'])==16
                               ,CSR_delta_agg['abs_WS']
                               ,np.sqrt(CSR_delta_agg['rslt_kl_M']))
CSR_delta_agg['Kb_H']=np.where(pd.to_numeric(CSR_delta_agg['RISK_FACTOR_BUCKET'])==16
                               ,CSR_delta_agg['abs_WS']
                               ,np.sqrt(CSR_delta_agg['rslt_kl_H']))
CSR_delta_agg['Kb_L']=np.where(pd.to_numeric(CSR_delta_agg['RISK_FACTOR_BUCKET'])==16
                               ,CSR_delta_agg['abs_WS']
                               ,np.sqrt(CSR_delta_agg['rslt_kl_L']))
CSR_delta_agg = CSR_delta_agg.rename({'WEIGHTED_SENSITIVITY':'Sb_M','rslt_kl_M':'Kb_M^2','rslt_kl_H':'Kb_H^2','rslt_kl_L':'Kb_L^2'},axis=1)


# In[208]:


CSR_delta_agg['Sb*_M']=np.maximum(np.minimum(CSR_delta_agg['Kb_M'],CSR_delta_agg['Sb_M']),-CSR_delta_agg['Kb_M'])
CSR_delta_agg['Sb*_H']=np.maximum(np.minimum(CSR_delta_agg['Kb_H'],CSR_delta_agg['Sb_H']),-CSR_delta_agg['Kb_H'])
CSR_delta_agg['Sb*_L']=np.maximum(np.minimum(CSR_delta_agg['Kb_L'],CSR_delta_agg['Sb_L']),-CSR_delta_agg['Kb_L'])


# In[209]:


CSR_delta_bc=CSR_delta_bc.merge(
    CSR_delta_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
    ,left_on=['Bucket_b'],right_on=['RISK_FACTOR_BUCKET'],how='left')

CSR_delta_bc=CSR_delta_bc.merge(
    CSR_delta_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
    ,left_on=['Bucket_c'],right_on=['RISK_FACTOR_BUCKET'],how='left')

CSR_delta_bc=CSR_delta_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)


# In[210]:


CSR_delta_bc['rslt_bc*_M']=CSR_delta_bc['Sb*_M']*CSR_delta_bc['Sc*_M']*CSR_delta_bc['Gamma_bc_M']
CSR_delta_bc['rslt_bc*_H']=CSR_delta_bc['Sb*_H']*CSR_delta_bc['Sc*_H']*CSR_delta_bc['Gamma_bc_H']
CSR_delta_bc['rslt_bc*_L']=CSR_delta_bc['Sb*_L']*CSR_delta_bc['Sc*_L']*CSR_delta_bc['Gamma_bc_L']


# In[211]:


csrd = pd.DataFrame([],columns=['GROUPING','RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=['0'])


# In[212]:


csrd_M_est=sum(CSR_delta_agg['Kb_M^2'])+sum(CSR_delta_bc['rslt_bc_M'])
csrd_M_1=np.sqrt(sum(CSR_delta_agg['Kb_M^2'])+sum(CSR_delta_bc['rslt_bc_M']))
csrd_M_2=np.sqrt(sum(CSR_delta_agg['Kb_M^2'])+sum(CSR_delta_bc['rslt_bc*_M']))


# In[213]:


csrd_H_est=sum(CSR_delta_agg['Kb_H^2'])+sum(CSR_delta_bc['rslt_bc_H'])
csrd_H_1=np.sqrt(sum(CSR_delta_agg['Kb_H^2'])+sum(CSR_delta_bc['rslt_bc_H']))
csrd_H_2=np.sqrt(sum(CSR_delta_agg['Kb_H^2'])+sum(CSR_delta_bc['rslt_bc*_H']))


# In[214]:


csrd_L_est=sum(CSR_delta_agg['Kb_L^2'])+sum(CSR_delta_bc['rslt_bc_L'])
csrd_L_1=np.sqrt(sum(CSR_delta_agg['Kb_L^2'])+sum(CSR_delta_bc['rslt_bc_L']))
csrd_L_2=np.sqrt(sum(CSR_delta_agg['Kb_L^2'])+sum(CSR_delta_bc['rslt_bc*_L']))


# In[215]:


csrd['RISK_FACTOR_CLASS']='CSR (non-sec)'
csrd['SENS_TYPE']='DELTA'
csrd['NORMAL']=np.where(csrd_M_est>=0,csrd_M_1,csrd_M_2)
csrd['HIGH']=np.where(csrd_H_est>=0,csrd_H_1,csrd_H_2)
csrd['LOW']=np.where(csrd_L_est>=0,csrd_L_1,csrd_L_2)


# In[216]:


csrd_1=CSR_delta[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]


# In[217]:


csrd_2=CSR_delta_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                      ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()


# In[218]:


csrd_3=CSR_delta_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                      ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()


# In[219]:


csrd_4=CSR_delta_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]


# In[220]:


csrd_decomp=csrd_1.merge(csrd_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET']
                          ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET'],how='left')\
.merge(csrd_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
.merge(csrd_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
.merge(csrd,on=['RISK_FACTOR_CLASS'],how='left')


# In[221]:


csrd_decomp=csrd_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','Bucket_b','GROUPING','SENS_TYPE'],axis=1)


# In[222]:


csrd_decomp['M_est']=csrd_M_est
csrd_decomp['H_est']=csrd_H_est
csrd_decomp['L_est']=csrd_L_est


# In[223]:


#1
csrd_decomp.loc[(csrd_decomp['M_est']>=0)&(csrd_decomp['Kb_M']>0),'pderp_M']=(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_M']+csrd_decomp['gammac_M'])/csrd_decomp['NORMAL']
csrd_decomp.loc[(csrd_decomp['H_est']>=0)&(csrd_decomp['Kb_H']>0),'pderp_H']=(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_H']+csrd_decomp['gammac_H'])/csrd_decomp['HIGH']
csrd_decomp.loc[(csrd_decomp['L_est']>=0)&(csrd_decomp['Kb_L']>0),'pderp_L']=(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_L']+csrd_decomp['gammac_L'])/csrd_decomp['LOW']

csrd_decomp.loc[(csrd_decomp['M_est']>=0)&(csrd_decomp['Kb_M']>0),'pderm_M']=csrd_decomp['pderp_M']
csrd_decomp.loc[(csrd_decomp['H_est']>=0)&(csrd_decomp['Kb_H']>0),'pderm_H']=csrd_decomp['pderp_H']
csrd_decomp.loc[(csrd_decomp['L_est']>=0)&(csrd_decomp['Kb_L']>0),'pderm_L']=csrd_decomp['pderp_L']

#2
csrd_decomp.loc[(csrd_decomp['M_est']>=0)&(csrd_decomp['Kb_M']==0),'pderp_M']=csrd_decomp['gammac_M']/csrd_decomp['NORMAL']
csrd_decomp.loc[(csrd_decomp['H_est']>=0)&(csrd_decomp['Kb_H']==0),'pderp_H']=csrd_decomp['gammac_H']/csrd_decomp['HIGH']
csrd_decomp.loc[(csrd_decomp['L_est']>=0)&(csrd_decomp['Kb_L']==0),'pderp_L']=csrd_decomp['gammac_L']/csrd_decomp['LOW']

csrd_decomp.loc[(csrd_decomp['M_est']>=0)&(csrd_decomp['Kb_M']==0),'pderm_M']=csrd_decomp['pderp_M']
csrd_decomp.loc[(csrd_decomp['H_est']>=0)&(csrd_decomp['Kb_H']==0),'pderm_H']=csrd_decomp['pderp_H']
csrd_decomp.loc[(csrd_decomp['L_est']>=0)&(csrd_decomp['Kb_L']==0),'pderm_L']=csrd_decomp['pderp_L']

#3
csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']==csrd_decomp['Kb_M'])
                ,'pderp_M']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_M'])*(1+1/csrd_decomp['Kb_M']*csrd_decomp['gammac_M']))/csrd_decomp['NORMAL']
csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']==csrd_decomp['Kb_H'])
                ,'pderp_H']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_H'])*(1+1/csrd_decomp['Kb_H']*csrd_decomp['gammac_H']))/csrd_decomp['HIGH']
csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']==csrd_decomp['Kb_L'])
                ,'pderp_L']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_L'])*(1+1/csrd_decomp['Kb_L']*csrd_decomp['gammac_L']))/csrd_decomp['LOW']

csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']==csrd_decomp['Kb_M'])
                ,'pderm_M']=csrd_decomp['pderp_M']
csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']==csrd_decomp['Kb_H'])
                ,'pderm_H']=csrd_decomp['pderp_H']
csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']==csrd_decomp['Kb_L'])
                ,'pderm_L']=csrd_decomp['pderp_L']

#4
csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']+csrd_decomp['Kb_M']==0)
                ,'pderp_M']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_M'])*(1-1/csrd_decomp['Kb_M']*csrd_decomp['gammac_M']))/csrd_decomp['NORMAL']
csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']+csrd_decomp['Kb_H']==0)
                ,'pderp_H']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_H'])*(1-1/csrd_decomp['Kb_H']*csrd_decomp['gammac_H']))/csrd_decomp['HIGH']
csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']+csrd_decomp['Kb_L']==0)
                ,'pderp_L']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_L'])*(1-1/csrd_decomp['Kb_L']*csrd_decomp['gammac_L']))/csrd_decomp['LOW']

csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']+csrd_decomp['Kb_M']==0)
                ,'pderm_M']=csrd_decomp['pderp_M']
csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']+csrd_decomp['Kb_H']==0)
                ,'pderm_H']=csrd_decomp['pderp_H']
csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']+csrd_decomp['Kb_L']==0)
                ,'pderm_L']=csrd_decomp['pderp_L']

#5
csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(abs(csrd_decomp['Sb*_M'])!=abs(csrd_decomp['Kb_M']))
                ,'pderp_M']=(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_M']+csrd_decomp['gammac_M'])/csrd_decomp['NORMAL']
csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(abs(csrd_decomp['Sb*_H'])!=abs(csrd_decomp['Kb_H']))
                ,'pderp_H']=(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_H']+csrd_decomp['gammac_H'])/csrd_decomp['HIGH']
csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(abs(csrd_decomp['Sb*_L'])!=abs(csrd_decomp['Kb_L']))
                ,'pderp_L']=(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_L']+csrd_decomp['gammac_L'])/csrd_decomp['LOW']

csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(abs(csrd_decomp['Sb*_M'])!=abs(csrd_decomp['Kb_M']))
                ,'pderm_M']=csrd_decomp['pderp_M']
csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(abs(csrd_decomp['Sb*_H'])!=abs(csrd_decomp['Kb_H']))
                ,'pderm_H']=csrd_decomp['pderp_H']
csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(abs(csrd_decomp['Sb*_L'])!=abs(csrd_decomp['Kb_L']))
                ,'pderm_L']=csrd_decomp['pderp_L']

#6
csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']==0),'pderp_M']=0
csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']==0),'pderp_H']=0
csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']==0),'pderp_L']=0

csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']==0),'pderm_M']=0
csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']==0),'pderm_H']=0
csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']==0),'pderm_L']=0


# In[224]:


##case 1
#csrd_decomp.loc[(csrd_decomp['M_est']>=0)&(csrd_decomp['Kb_M']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16),'pderp_M']=\
#(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_M']+csrd_decomp['gammac_M'])/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']>=0)&(csrd_decomp['Kb_H']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16),'pderp_H']=\
#(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_H']+csrd_decomp['gammac_H'])/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']>=0)&(csrd_decomp['Kb_L']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16),'pderp_L']=\
#(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_L']+csrd_decomp['gammac_L'])/csrd_decomp['LOW']
#
#csrd_decomp.loc[(csrd_decomp['M_est']>=0)&(csrd_decomp['Kb_M']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16),'pderm_M']=csrd_decomp['pderp_M']
#csrd_decomp.loc[(csrd_decomp['H_est']>=0)&(csrd_decomp['Kb_H']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16),'pderm_H']=csrd_decomp['pderp_H']
#csrd_decomp.loc[(csrd_decomp['L_est']>=0)&(csrd_decomp['Kb_L']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16),'pderm_L']=csrd_decomp['pderp_L']
#
#csrd_decomp.loc[(csrd_decomp['M_est']>=0)&(csrd_decomp['Kb_M']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16),'pderp_M']=\
#(csrd_decomp['Kb_M']+csrd_decomp['gammac_M'])/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']>=0)&(csrd_decomp['Kb_H']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16),'pderp_H']=\
#(csrd_decomp['Kb_H']+csrd_decomp['gammac_H'])/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']>=0)&(csrd_decomp['Kb_L']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16),'pderp_L']=\
#(csrd_decomp['Kb_L']+csrd_decomp['gammac_L'])/csrd_decomp['LOW']
#
#csrd_decomp.loc[(csrd_decomp['M_est']>=0)&(csrd_decomp['Kb_M']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16),'pderm_M']=\
#(-csrd_decomp['Kb_M']+csrd_decomp['gammac_M'])/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']>=0)&(csrd_decomp['Kb_H']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16),'pderm_H']=\
#(-csrd_decomp['Kb_H']+csrd_decomp['gammac_H'])/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']>=0)&(csrd_decomp['Kb_L']>0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16),'pderm_L']=\
#(-csrd_decomp['Kb_L']+csrd_decomp['gammac_L'])/csrd_decomp['LOW']


# In[225]:


##case 2
#csrd_decomp.loc[(csrd_decomp['M_est']>=0)&(csrd_decomp['Kb_M']==0),'pderp_M']=csrd_decomp['gammac_M']/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']>=0)&(csrd_decomp['Kb_H']==0),'pderp_H']=csrd_decomp['gammac_H']/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']>=0)&(csrd_decomp['Kb_L']==0),'pderp_L']=csrd_decomp['gammac_L']/csrd_decomp['LOW']
#
#csrd_decomp.loc[(csrd_decomp['M_est']>=0)&(csrd_decomp['Kb_M']==0),'pderm_M']=csrd_decomp['pderp_M']
#csrd_decomp.loc[(csrd_decomp['H_est']>=0)&(csrd_decomp['Kb_H']==0),'pderm_H']=csrd_decomp['pderp_H']
#csrd_decomp.loc[(csrd_decomp['L_est']>=0)&(csrd_decomp['Kb_L']==0),'pderm_L']=csrd_decomp['pderp_L']


# In[226]:


##case 3
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']==csrd_decomp['Kb_M'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderp_M']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_M'])*(1+1/csrd_decomp['Kb_M']*csrd_decomp['gammac_M']))/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']==csrd_decomp['Kb_H'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderp_H']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_H'])*(1+1/csrd_decomp['Kb_H']*csrd_decomp['gammac_H']))/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']==csrd_decomp['Kb_L'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderp_L']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_L'])*(1+1/csrd_decomp['Kb_L']*csrd_decomp['gammac_L']))/csrd_decomp['LOW']
#
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']==csrd_decomp['Kb_M'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderm_M']=csrd_decomp['pderp_M']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']==csrd_decomp['Kb_H'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderm_H']=csrd_decomp['pderp_H']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']==csrd_decomp['Kb_L'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderm_L']=csrd_decomp['pderp_L']
#
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']==csrd_decomp['Kb_M'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderp_M']=(csrd_decomp['Kb_M']*(1+1/csrd_decomp['Kb_M']*csrd_decomp['gammac_M']))/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']==csrd_decomp['Kb_H'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderp_H']=(csrd_decomp['Kb_H']*(1+1/csrd_decomp['Kb_H']*csrd_decomp['gammac_H']))/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']==csrd_decomp['Kb_L'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderp_L']=(csrd_decomp['Kb_L']*(1+1/csrd_decomp['Kb_L']*csrd_decomp['gammac_L']))/csrd_decomp['LOW']
#
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']==csrd_decomp['Kb_M'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderm_M']=(-csrd_decomp['Kb_M']*(1+1/csrd_decomp['Kb_M']*csrd_decomp['gammac_M']))/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']==csrd_decomp['Kb_H'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderm_H']=(-csrd_decomp['Kb_H']*(1+1/csrd_decomp['Kb_H']*csrd_decomp['gammac_H']))/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']==csrd_decomp['Kb_L'])&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderm_L']=(-csrd_decomp['Kb_L']*(1+1/csrd_decomp['Kb_L']*csrd_decomp['gammac_L']))/csrd_decomp['LOW']
#


# In[227]:


##case 4
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']+csrd_decomp['Kb_M']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderp_M']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_M'])*(1-1/csrd_decomp['Kb_M']*csrd_decomp['gammac_M']))/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']+csrd_decomp['Kb_H']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderp_H']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_H'])*(1-1/csrd_decomp['Kb_H']*csrd_decomp['gammac_H']))/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']+csrd_decomp['Kb_L']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderp_L']=((csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_L'])*(1-1/csrd_decomp['Kb_L']*csrd_decomp['gammac_L']))/csrd_decomp['LOW']
#
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']+csrd_decomp['Kb_M']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderm_M']=csrd_decomp['pderp_M']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']+csrd_decomp['Kb_H']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderm_H']=csrd_decomp['pderp_H']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']+csrd_decomp['Kb_L']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderm_L']=csrd_decomp['pderp_L']
#
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']+csrd_decomp['Kb_M']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderp_M']=(csrd_decomp['Kb_M']*(1-1/csrd_decomp['Kb_M']*csrd_decomp['gammac_M']))/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']+csrd_decomp['Kb_H']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderp_H']=(csrd_decomp['Kb_H']*(1-1/csrd_decomp['Kb_H']*csrd_decomp['gammac_H']))/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']+csrd_decomp['Kb_L']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderp_L']=(csrd_decomp['Kb_L']*(1-1/csrd_decomp['Kb_L']*csrd_decomp['gammac_L']))/csrd_decomp['LOW']
#
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(csrd_decomp['Sb*_M']+csrd_decomp['Kb_M']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderm_M']=(-csrd_decomp['Kb_M']*(1-1/csrd_decomp['Kb_M']*csrd_decomp['gammac_M']))/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(csrd_decomp['Sb*_H']+csrd_decomp['Kb_H']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderm_H']=(-csrd_decomp['Kb_H']*(1-1/csrd_decomp['Kb_H']*csrd_decomp['gammac_H']))/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(csrd_decomp['Sb*_L']+csrd_decomp['Kb_L']==0)&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderm_L']=(-csrd_decomp['Kb_L']*(1-1/csrd_decomp['Kb_L']*csrd_decomp['gammac_L']))/csrd_decomp['LOW']
#


# In[228]:


##case 5
#
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(abs(csrd_decomp['Sb*_M'])!=abs(csrd_decomp['Kb_M']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderp_M']=(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_M']+csrd_decomp['gammac_M'])/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(abs(csrd_decomp['Sb*_H'])!=abs(csrd_decomp['Kb_H']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderp_H']=(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_H']+csrd_decomp['gammac_H'])/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(abs(csrd_decomp['Sb*_L'])!=abs(csrd_decomp['Kb_L']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderp_L']=(csrd_decomp['WEIGHTED_SENSITIVITY']+csrd_decomp['rhol_L']+csrd_decomp['gammac_L'])/csrd_decomp['LOW']
#
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(abs(csrd_decomp['Sb*_M'])!=abs(csrd_decomp['Kb_M']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderm_M']=csrd_decomp['pderp_M']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(abs(csrd_decomp['Sb*_H'])!=abs(csrd_decomp['Kb_H']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderm_H']=csrd_decomp['pderp_H']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(abs(csrd_decomp['Sb*_L'])!=abs(csrd_decomp['Kb_L']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])!=16)
#                ,'pderm_L']=csrd_decomp['pderp_L']
#
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(abs(csrd_decomp['Sb*_M'])!=abs(csrd_decomp['Kb_M']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderp_M']=(csrd_decomp['Kb_M']+csrd_decomp['gammac_M'])/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(abs(csrd_decomp['Sb*_H'])!=abs(csrd_decomp['Kb_H']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderp_H']=(csrd_decomp['Kb_H']+csrd_decomp['gammac_H'])/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(abs(csrd_decomp['Sb*_L'])!=abs(csrd_decomp['Kb_L']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderp_L']=(csrd_decomp['Kb_L']+csrd_decomp['gammac_L'])/csrd_decomp['LOW']
#
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']>0)&(abs(csrd_decomp['Sb*_M'])!=abs(csrd_decomp['Kb_M']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderm_M']=(-csrd_decomp['Kb_M']+csrd_decomp['gammac_M'])/csrd_decomp['NORMAL']
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']>0)&(abs(csrd_decomp['Sb*_H'])!=abs(csrd_decomp['Kb_H']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderm_H']=(-csrd_decomp['Kb_H']+csrd_decomp['gammac_H'])/csrd_decomp['HIGH']
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']>0)&(abs(csrd_decomp['Sb*_L'])!=abs(csrd_decomp['Kb_L']))&(pd.to_numeric(csrd_decomp['RISK_FACTOR_BUCKET'])==16)
#                ,'pderm_L']=(-csrd_decomp['Kb_L']+csrd_decomp['gammac_L'])/csrd_decomp['LOW']


# In[229]:


##case 6
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']==0),'pderp_M']=0
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']==0),'pderp_H']=0
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']==0),'pderp_L']=0
#
#csrd_decomp.loc[(csrd_decomp['M_est']<0)&(csrd_decomp['Kb_M']==0),'pderm_M']=0
#csrd_decomp.loc[(csrd_decomp['H_est']<0)&(csrd_decomp['Kb_H']==0),'pderm_H']=0
#csrd_decomp.loc[(csrd_decomp['L_est']<0)&(csrd_decomp['Kb_L']==0),'pderm_L']=0


# In[230]:


csrd_decomp=csrd_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','pderp_M','pderp_H','pderp_L','pderm_M','pderm_H','pderm_L']]


# In[231]:


csrd_decomp_rslt=CSR_RawData[(CSR_RawData.SENSITIVITY_TYPE=='Delta')].merge(csrd_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'],how='left')


# In[232]:


csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY>=0),'pder_M']=csrd_decomp_rslt.pderp_M
csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY>=0),'pder_H']=csrd_decomp_rslt.pderp_H
csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY>=0),'pder_L']=csrd_decomp_rslt.pderp_L
csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY<0),'pder_M']=csrd_decomp_rslt.pderm_M
csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY<0),'pder_H']=csrd_decomp_rslt.pderm_H
csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY<0),'pder_L']=csrd_decomp_rslt.pderm_L


# In[233]:


#sum(csrd_decomp_rslt['WEIGHTED_SENSITIVITY']*csrd_decomp_rslt['pder_M'])


# In[234]:


#sum(csrd_decomp_rslt['WEIGHTED_SENSITIVITY']*csrd_decomp_rslt['pder_H'])


# In[235]:


#sum(csrd_decomp_rslt['WEIGHTED_SENSITIVITY']*csrd_decomp_rslt['pder_L'])


# In[236]:


#csrd


# In[ ]:





# In[ ]:





# In[237]:


CSRNC_Weights = params.parse('CSRNonCTP_Weights')
CSRNC_Rho_Tranch = 0.4
CSRNC_Rho_Tenor = 0.8
CSRNC_Rho_Basis = 0.999
CSRNC_Gamma = params.parse('CSRNonCTP_Gamma')
CSRNC_LH = 120
CSRNC_vega_rw = 1


# In[254]:


CSRNC_Gamma['Bucket_b']=CSRNC_Gamma['Bucket_b'].astype(str)
CSRNC_Gamma['Bucket_c']=CSRNC_Gamma['Bucket_c'].astype(str)


# In[238]:


CSRNC_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='CSR (non-ctp)')]


# In[239]:


CSRNC_Position=CSRNC_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2'
                              ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE','SEC_TRANCHE'
                              ,'SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]


# In[240]:


CSRNC_delta=CSRNC_Position[(CSRNC_Position['SENSITIVITY_TYPE']=='Delta')]


# In[241]:


CSRNC_delta['abs_WS']=abs(CSRNC_Position['WEIGHTED_SENSITIVITY'])


# In[242]:


CSRNC_delta=CSRNC_delta.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2'
                                 ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE','SEC_TRANCHE'
                                 ,'SENSITIVITY_TYPE'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum','abs_WS':'sum'}).reset_index()


# In[243]:


CSRNC_delta_kl = CSRNC_delta.rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_K'
                                      ,'RISK_FACTOR_TYPE':'RISK_FACTOR_TYPE_K','SEC_TRANCHE':'SEC_TRANCHE_K'
                                      ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_K'},axis=1
                                   ).merge(CSRNC_delta[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE'
                                                        ,'SEC_TRANCHE','WEIGHTED_SENSITIVITY']]
                                           .rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_L'
                                                    ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_L'
                                                    ,'RISK_FACTOR_TYPE':'RISK_FACTOR_TYPE_L'
                                                    ,'SEC_TRANCHE':'SEC_TRANCHE_L'
                                                    ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_L'},axis=1)
                                           ,on='RISK_FACTOR_BUCKET',how='left')


# In[244]:


CSRNC_delta_kl.loc[CSRNC_delta_kl['SEC_TRANCHE_K']==CSRNC_delta_kl['SEC_TRANCHE_L'],'Rho_Tranch']=1
CSRNC_delta_kl.loc[CSRNC_delta_kl['SEC_TRANCHE_K']!=CSRNC_delta_kl['SEC_TRANCHE_L'],'Rho_Tranch']=CSRNC_Rho_Tranch
CSRNC_delta_kl.loc[CSRNC_delta_kl['RISK_FACTOR_VERTEX_1_K']==CSRNC_delta_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=1
CSRNC_delta_kl.loc[CSRNC_delta_kl['RISK_FACTOR_VERTEX_1_K']!=CSRNC_delta_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=CSRNC_Rho_Tenor
CSRNC_delta_kl.loc[CSRNC_delta_kl['RISK_FACTOR_TYPE_K']==CSRNC_delta_kl['RISK_FACTOR_TYPE_L'],'Rho_Basis']=1
CSRNC_delta_kl.loc[CSRNC_delta_kl['RISK_FACTOR_TYPE_K']!=CSRNC_delta_kl['RISK_FACTOR_TYPE_L'],'Rho_Basis']=CSRNC_Rho_Basis


# In[245]:


CSRNC_delta_kl['Rho_kl_M']=CSRNC_delta_kl['Rho_Tranch']*CSRNC_delta_kl['Rho_Tenor']*CSRNC_delta_kl['Rho_Basis']
CSRNC_delta_kl['Rho_kl_H']=np.minimum(1,High_Multipler*CSRNC_delta_kl['Rho_kl_M'])
CSRNC_delta_kl['Rho_kl_L']=np.maximum(Low_Multipler1*CSRNC_delta_kl['Rho_kl_M']-1,Low_Multipler2*CSRNC_delta_kl['Rho_kl_M'])


# In[246]:


CSRNC_delta_kl['rslt_kl_M']=CSRNC_delta_kl['WEIGHTED_SENSITIVITY_K']*CSRNC_delta_kl['WEIGHTED_SENSITIVITY_L']*CSRNC_delta_kl['Rho_kl_M']
CSRNC_delta_kl['rslt_kl_H']=CSRNC_delta_kl['WEIGHTED_SENSITIVITY_K']*CSRNC_delta_kl['WEIGHTED_SENSITIVITY_L']*CSRNC_delta_kl['Rho_kl_H']
CSRNC_delta_kl['rslt_kl_L']=CSRNC_delta_kl['WEIGHTED_SENSITIVITY_K']*CSRNC_delta_kl['WEIGHTED_SENSITIVITY_L']*CSRNC_delta_kl['Rho_kl_L']


# In[247]:


CSRNC_delta_kl.loc[(CSRNC_delta_kl.RISK_FACTOR_ID_K==CSRNC_delta_kl.RISK_FACTOR_ID_L)
                  &(CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_K==CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=0
CSRNC_delta_kl.loc[(CSRNC_delta_kl.RISK_FACTOR_ID_K==CSRNC_delta_kl.RISK_FACTOR_ID_L)
                  &(CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_K==CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=0
CSRNC_delta_kl.loc[(CSRNC_delta_kl.RISK_FACTOR_ID_K==CSRNC_delta_kl.RISK_FACTOR_ID_L)
                  &(CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_K==CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=0

CSRNC_delta_kl.loc[(CSRNC_delta_kl.RISK_FACTOR_ID_K!=CSRNC_delta_kl.RISK_FACTOR_ID_L)
                  |(CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_K!=CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=CSRNC_delta_kl['WEIGHTED_SENSITIVITY_L']*CSRNC_delta_kl['Rho_kl_M']
CSRNC_delta_kl.loc[(CSRNC_delta_kl.RISK_FACTOR_ID_K!=CSRNC_delta_kl.RISK_FACTOR_ID_L)
                  |(CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_K!=CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=CSRNC_delta_kl['WEIGHTED_SENSITIVITY_L']*CSRNC_delta_kl['Rho_kl_H']
CSRNC_delta_kl.loc[(CSRNC_delta_kl.RISK_FACTOR_ID_K!=CSRNC_delta_kl.RISK_FACTOR_ID_L)
                  |(CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_K!=CSRNC_delta_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=CSRNC_delta_kl['WEIGHTED_SENSITIVITY_L']*CSRNC_delta_kl['Rho_kl_L']


# In[248]:


CSRNC_delta_agg=CSRNC_delta.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False
                                   ).agg({'WEIGHTED_SENSITIVITY':'sum','abs_WS':'sum'}).reset_index()


# In[249]:


CSRNC_delta_bc=CSRNC_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b','abs_WS':'abs_WS_b'},axis=1
                                     ).merge(CSRNC_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c','WEIGHTED_SENSITIVITY':'WS_c','abs_WS':'abs_WS_c'}
                                                                    ,axis=1),on='RISK_FACTOR_CLASS',how='left').reset_index(drop=True)


# In[250]:


CSRNC_delta_bc = CSRNC_delta_bc.loc[(CSRNC_delta_bc.Bucket_b!=CSRNC_delta_bc.Bucket_c),:].reset_index(drop=True)


# In[255]:


CSRNC_delta_bc=CSRNC_delta_bc.merge(CSRNC_Gamma.rename({'Gamma_bc':'Gamma_bc_M'},axis=1),on=['Bucket_b','Bucket_c'],how='left')
CSRNC_delta_bc['Gamma_bc_H']=np.minimum(1,High_Multipler*CSRNC_delta_bc['Gamma_bc_M'])
CSRNC_delta_bc['Gamma_bc_L']=np.maximum(Low_Multipler1*CSRNC_delta_bc['Gamma_bc_M']-1,Low_Multipler2*CSRNC_delta_bc['Gamma_bc_M'])


# In[256]:


CSRNC_delta_bc['rslt_bc_M']=CSRNC_delta_bc['WS_b']*CSRNC_delta_bc['WS_c']*CSRNC_delta_bc['Gamma_bc_M']
CSRNC_delta_bc['rslt_bc_H']=CSRNC_delta_bc['WS_b']*CSRNC_delta_bc['WS_c']*CSRNC_delta_bc['Gamma_bc_H']
CSRNC_delta_bc['rslt_bc_L']=CSRNC_delta_bc['WS_b']*CSRNC_delta_bc['WS_c']*CSRNC_delta_bc['Gamma_bc_L']


# In[257]:


CSRNC_delta_bc['gammac_M']=CSRNC_delta_bc.WS_c*CSRNC_delta_bc.Gamma_bc_M
CSRNC_delta_bc['gammac_H']=CSRNC_delta_bc.WS_c*CSRNC_delta_bc.Gamma_bc_H
CSRNC_delta_bc['gammac_L']=CSRNC_delta_bc.WS_c*CSRNC_delta_bc.Gamma_bc_L


# In[258]:


CSRNC_delta_agg=CSRNC_delta_agg.merge(
    CSRNC_delta_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']],on='RISK_FACTOR_BUCKET',how='left'
).groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY','abs_WS']
                                      ,dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()


# In[259]:


CSRNC_delta_agg['Sb_H']=CSRNC_delta_agg['WEIGHTED_SENSITIVITY']
CSRNC_delta_agg['Sb_L']=CSRNC_delta_agg['WEIGHTED_SENSITIVITY']
CSRNC_delta_agg['Kb_M']=np.where(pd.to_numeric(CSRNC_delta_agg.RISK_FACTOR_BUCKET)==25
                                               ,CSRNC_delta_agg['abs_WS']
                                               ,np.sqrt(CSRNC_delta_agg['rslt_kl_M']))
CSRNC_delta_agg['Kb_H']=np.where(pd.to_numeric(CSRNC_delta_agg.RISK_FACTOR_BUCKET)==25
                                               ,CSRNC_delta_agg['abs_WS']
                                               ,np.sqrt(CSRNC_delta_agg['rslt_kl_H']))
CSRNC_delta_agg['Kb_L']=np.where(pd.to_numeric(CSRNC_delta_agg.RISK_FACTOR_BUCKET)==25
                                               ,CSRNC_delta_agg['abs_WS']
                                               ,np.sqrt(CSRNC_delta_agg['rslt_kl_L']))
CSRNC_delta_agg = CSRNC_delta_agg.rename({'WEIGHTED_SENSITIVITY':'Sb_M','rslt_kl_M':'Kb_M^2','rslt_kl_H':'Kb_H^2','rslt_kl_L':'Kb_L^2'},axis=1)


# In[260]:


CSRNC_delta_agg['Sb*_M']=np.maximum(np.minimum(CSRNC_delta_agg['Kb_M'],CSRNC_delta_agg['Sb_M']),-CSRNC_delta_agg['Kb_M'])
CSRNC_delta_agg['Sb*_H']=np.maximum(np.minimum(CSRNC_delta_agg['Kb_H'],CSRNC_delta_agg['Sb_H']),-CSRNC_delta_agg['Kb_H'])
CSRNC_delta_agg['Sb*_L']=np.maximum(np.minimum(CSRNC_delta_agg['Kb_L'],CSRNC_delta_agg['Sb_L']),-CSRNC_delta_agg['Kb_L'])


# In[261]:


CSRNC_delta_bc=CSRNC_delta_bc.merge(
    CSRNC_delta_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
    ,left_on='Bucket_b',right_on='RISK_FACTOR_BUCKET',how='left')

CSRNC_delta_bc=CSRNC_delta_bc.merge(
    CSRNC_delta_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
    ,left_on='Bucket_c',right_on='RISK_FACTOR_BUCKET',how='left')

CSRNC_delta_bc=CSRNC_delta_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)


# In[262]:


CSRNC_delta_bc['rslt_bc*_M']=CSRNC_delta_bc['Sb*_M']*CSRNC_delta_bc['Sc*_M']*CSRNC_delta_bc['Gamma_bc_M']
CSRNC_delta_bc['rslt_bc*_H']=CSRNC_delta_bc['Sb*_H']*CSRNC_delta_bc['Sc*_H']*CSRNC_delta_bc['Gamma_bc_H']
CSRNC_delta_bc['rslt_bc*_L']=CSRNC_delta_bc['Sb*_L']*CSRNC_delta_bc['Sc*_L']*CSRNC_delta_bc['Gamma_bc_L']


# In[263]:


csrncd = pd.DataFrame([],columns=['GROUPING','RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=['0'])


# In[264]:


csrncd_M_est=sum(CSRNC_delta_agg['Kb_M^2'])+sum(CSRNC_delta_bc['rslt_bc_M'])
csrncd_M_1=np.sqrt(sum(CSRNC_delta_agg['Kb_M^2'])+sum(CSRNC_delta_bc['rslt_bc_M']))
csrncd_M_2=np.sqrt(sum(CSRNC_delta_agg['Kb_M^2'])+sum(CSRNC_delta_bc['rslt_bc*_M']))

csrncd_H_est=sum(CSRNC_delta_agg['Kb_H^2'])+sum(CSRNC_delta_bc['rslt_bc_H'])
csrncd_H_1=np.sqrt(sum(CSRNC_delta_agg['Kb_H^2'])+sum(CSRNC_delta_bc['rslt_bc_H']))
csrncd_H_2=np.sqrt(sum(CSRNC_delta_agg['Kb_H^2'])+sum(CSRNC_delta_bc['rslt_bc*_H']))

csrncd_L_est=sum(CSRNC_delta_agg['Kb_L^2'])+sum(CSRNC_delta_bc['rslt_bc_L'])
csrncd_L_1=np.sqrt(sum(CSRNC_delta_agg['Kb_L^2'])+sum(CSRNC_delta_bc['rslt_bc_L']))
csrncd_L_2=np.sqrt(sum(CSRNC_delta_agg['Kb_L^2'])+sum(CSRNC_delta_bc['rslt_bc*_L']))


# In[265]:


csrncd['RISK_FACTOR_CLASS']='CSR (non-ctp)'
csrncd['SENS_TYPE']='DELTA'
csrncd['NORMAL']=np.where(csrncd_M_est>=0,csrncd_M_1,csrncd_M_2)
csrncd['HIGH']=np.where(csrncd_H_est>=0,csrncd_H_1,csrncd_H_2)
csrncd['LOW']=np.where(csrncd_L_est>=0,csrncd_L_1,csrncd_L_2)


# In[266]:


csrncd_1=CSRNC_delta[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]


# In[267]:


csrncd_2=CSRNC_delta_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                      ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()


# In[268]:


csrncd_3=CSRNC_delta_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                      ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()


# In[269]:


csrncd_4=CSRNC_delta_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]


# In[270]:


csrncd_decomp=csrncd_1.merge(csrncd_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET']
                          ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET'],how='left')\
.merge(csrncd_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
.merge(csrncd_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
.merge(csrncd,on=['RISK_FACTOR_CLASS'],how='left')


# In[271]:


csrncd_decomp=csrncd_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','Bucket_b','GROUPING','SENS_TYPE'],axis=1)


# In[272]:


csrncd_decomp['M_est']=csrncd_M_est
csrncd_decomp['H_est']=csrncd_H_est
csrncd_decomp['L_est']=csrncd_L_est


# In[273]:


#case 1
csrncd_decomp.loc[(csrncd_decomp['M_est']>=0)&(csrncd_decomp['Kb_M']>0),'pder_M']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_M']+csrncd_decomp['gammac_M'])/csrncd_decomp['NORMAL']

csrncd_decomp.loc[(csrncd_decomp['H_est']>=0)&(csrncd_decomp['Kb_H']>0),'pder_H']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_H']+csrncd_decomp['gammac_H'])/csrncd_decomp['HIGH']

csrncd_decomp.loc[(csrncd_decomp['L_est']>=0)&(csrncd_decomp['Kb_L']>0),'pder_L']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_L']+csrncd_decomp['gammac_L'])/csrncd_decomp['LOW']


# In[274]:


#case 2
csrncd_decomp.loc[(csrncd_decomp['M_est']>=0)&(csrncd_decomp['Kb_M']==0),'pder_M']=csrncd_decomp['gammac_M']/csrncd_decomp['NORMAL']

csrncd_decomp.loc[(csrncd_decomp['H_est']>=0)&(csrncd_decomp['Kb_H']==0),'pder_H']=csrncd_decomp['gammac_H']/csrncd_decomp['HIGH']

csrncd_decomp.loc[(csrncd_decomp['L_est']>=0)&(csrncd_decomp['Kb_L']==0),'pder_L']=csrncd_decomp['gammac_L']/csrncd_decomp['LOW']


# In[275]:


#case 3
csrncd_decomp.loc[(csrncd_decomp['M_est']<0)&(csrncd_decomp['Kb_M']>0)&(csrncd_decomp['Sb*_M']==csrncd_decomp['Kb_M']),'pder_M']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_M'])*(1+1/csrncd_decomp['Kb_M']*csrncd_decomp['gammac_M']))/csrncd_decomp['NORMAL']

csrncd_decomp.loc[(csrncd_decomp['H_est']<0)&(csrncd_decomp['Kb_H']>0)&(csrncd_decomp['Sb*_H']==csrncd_decomp['Kb_H']),'pder_H']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_H'])*(1+1/csrncd_decomp['Kb_H']*csrncd_decomp['gammac_H']))/csrncd_decomp['HIGH']

csrncd_decomp.loc[(csrncd_decomp['L_est']<0)&(csrncd_decomp['Kb_L']>0)&(csrncd_decomp['Sb*_L']==csrncd_decomp['Kb_L']),'pder_L']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_L'])*(1+1/csrncd_decomp['Kb_L']*csrncd_decomp['gammac_L']))/csrncd_decomp['LOW']


# In[276]:


#case 4
csrncd_decomp.loc[(csrncd_decomp['M_est']<0)&(csrncd_decomp['Kb_M']>0)&(csrncd_decomp['Sb*_M']+csrncd_decomp['Kb_M']==0),'pder_M']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_M'])*(1-1/csrncd_decomp['Kb_M']*csrncd_decomp['gammac_M']))/csrncd_decomp['NORMAL']

csrncd_decomp.loc[(csrncd_decomp['H_est']<0)&(csrncd_decomp['Kb_H']>0)&(csrncd_decomp['Sb*_H']+csrncd_decomp['Kb_H']==0),'pder_H']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_H'])*(1-1/csrncd_decomp['Kb_H']*csrncd_decomp['gammac_H']))/csrncd_decomp['HIGH']

csrncd_decomp.loc[(csrncd_decomp['L_est']<0)&(csrncd_decomp['Kb_L']>0)&(csrncd_decomp['Sb*_L']+csrncd_decomp['Kb_L']==0),'pder_L']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_L'])*(1-1/csrncd_decomp['Kb_L']*csrncd_decomp['gammac_L']))/csrncd_decomp['LOW']


# In[277]:


#case 5
csrncd_decomp.loc[(csrncd_decomp['M_est']<0)&(csrncd_decomp['Kb_M']>0)&(abs(csrncd_decomp['Sb*_M'])!=abs(csrncd_decomp['Kb_M'])),'pder_M']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_M']+csrncd_decomp['gammac_M'])/csrncd_decomp['NORMAL']

csrncd_decomp.loc[(csrncd_decomp['H_est']<0)&(csrncd_decomp['Kb_H']>0)&(abs(csrncd_decomp['Sb*_H'])!=abs(csrncd_decomp['Kb_H'])),'pder_H']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_H']+csrncd_decomp['gammac_H'])/csrncd_decomp['HIGH']

csrncd_decomp.loc[(csrncd_decomp['L_est']<0)&(csrncd_decomp['Kb_L']>0)&(abs(csrncd_decomp['Sb*_L'])!=abs(csrncd_decomp['Kb_L'])),'pder_L']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_L']+csrncd_decomp['gammac_L'])/csrncd_decomp['LOW']


# In[278]:


#case 6
csrncd_decomp.loc[(csrncd_decomp['M_est']<0)&(csrncd_decomp['Kb_M']==0),'pder_M']=0

csrncd_decomp.loc[(csrncd_decomp['H_est']<0)&(csrncd_decomp['Kb_H']==0),'pder_H']=0

csrncd_decomp.loc[(csrncd_decomp['L_est']<0)&(csrncd_decomp['Kb_L']==0),'pder_L']=0


# In[279]:


csrncd_decomp=csrncd_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]


# In[280]:


csrncd_decomp_rslt=CSRNC_RawData[(CSRNC_RawData.SENSITIVITY_TYPE=='Delta')].merge(csrncd_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'],how='left')


# In[281]:


#sum(csrncd_decomp_rslt['WEIGHTED_SENSITIVITY']*csrncd_decomp_rslt['pder_M'])


# In[282]:


#sum(csrncd_decomp_rslt['WEIGHTED_SENSITIVITY']*csrncd_decomp_rslt['pder_H'])


# In[283]:


#sum(csrncd_decomp_rslt['WEIGHTED_SENSITIVITY']*csrncd_decomp_rslt['pder_L'])


# In[284]:


#csrncd


# In[ ]:





# In[ ]:





# In[ ]:





# In[285]:


CMTY_Weights = params.parse('Commodity_Weights')
CMTY_Rho_Cty = params.parse('Commodity_Rho')
CMTY_Rho_Tenor = 0.99 #time differs
CMTY_Rho_Basis = 0.999 #location differs
CMTY_Gamma = params.parse('Commodity_Gamma')
CMTY_LH = 120
CMTY_vega_rw = 1


# In[296]:


CMTY_Rho_Cty['RISK_FACTOR_BUCKET']=CMTY_Rho_Cty['RISK_FACTOR_BUCKET'].astype(str)
CMTY_Gamma['Bucket_b']=CMTY_Gamma['Bucket_b'].astype(str)
CMTY_Gamma['Bucket_c']=CMTY_Gamma['Bucket_c'].astype(str)


# In[288]:


CMTY_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='CMTY')]


# In[289]:


CMTY_Position = CMTY_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                              'RISK_FACTOR_BUCKET','COMM_ASSET',
                              'COMM_LOCATION','SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]


# In[290]:


CMTY_Position = CMTY_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                       'RISK_FACTOR_BUCKET','COMM_ASSET',
                                       'COMM_LOCATION','SENSITIVITY_TYPE']
                                      ,dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()


# In[291]:


CMTY_delta = CMTY_Position[(CMTY_Position['SENSITIVITY_TYPE']=='Delta')]


# In[292]:


CMTY_delta_kl = CMTY_delta.rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_K'
                                   ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_K'
                                   ,'COMM_ASSET':'COMM_ASSET_K'
                                   ,'COMM_LOCATION':'COMM_LOCATION_K'
                                   ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_K'},axis=1
                                 ).merge(CMTY_delta[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'
                                                     ,'COMM_ASSET'
                                                     ,'COMM_LOCATION','WEIGHTED_SENSITIVITY']]
                                         .rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_L'
                                                  ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_L'
                                                  ,'COMM_ASSET':'COMM_ASSET_L'
                                                  ,'COMM_LOCATION':'COMM_LOCATION_L'
                                                  ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_L'},axis=1)
                                         ,on =['RISK_FACTOR_BUCKET'],how='left')


# In[297]:


CMTY_delta_kl = CMTY_delta_kl.merge(CMTY_Rho_Cty,on='RISK_FACTOR_BUCKET',how='left')


# In[298]:


CMTY_delta_kl.loc[CMTY_delta_kl['COMM_ASSET_K']==CMTY_delta_kl['COMM_ASSET_L'],'Rho_Cty']=1
CMTY_delta_kl.loc[CMTY_delta_kl['COMM_ASSET_K']!=CMTY_delta_kl['COMM_ASSET_L'],'Rho_Cty']=CMTY_delta_kl['Rho']
CMTY_delta_kl.loc[CMTY_delta_kl['RISK_FACTOR_VERTEX_1_K']==CMTY_delta_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=1
CMTY_delta_kl.loc[CMTY_delta_kl['RISK_FACTOR_VERTEX_1_K']!=CMTY_delta_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=CMTY_Rho_Tenor
CMTY_delta_kl.loc[CMTY_delta_kl['RISK_FACTOR_ID_K']==CMTY_delta_kl['RISK_FACTOR_ID_L'],'Rho_Basis']=1
CMTY_delta_kl.loc[CMTY_delta_kl['RISK_FACTOR_ID_K']!=CMTY_delta_kl['RISK_FACTOR_ID_L'],'Rho_Basis']=CMTY_Rho_Basis


# In[299]:


CMTY_delta_kl['Rho_kl_M']=CMTY_delta_kl['Rho_Cty']*CMTY_delta_kl['Rho_Tenor']*CMTY_delta_kl['Rho_Basis']
CMTY_delta_kl['Rho_kl_H']=np.minimum(1,High_Multipler*CMTY_delta_kl['Rho_kl_M'])
CMTY_delta_kl['Rho_kl_L']=np.maximum(Low_Multipler1*CMTY_delta_kl['Rho_kl_M']-1,Low_Multipler2*CMTY_delta_kl['Rho_kl_M'])


# In[300]:


CMTY_delta_kl['rslt_kl_M']=CMTY_delta_kl['WEIGHTED_SENSITIVITY_K']*CMTY_delta_kl['WEIGHTED_SENSITIVITY_L']*CMTY_delta_kl['Rho_kl_M']
CMTY_delta_kl['rslt_kl_H']=CMTY_delta_kl['WEIGHTED_SENSITIVITY_K']*CMTY_delta_kl['WEIGHTED_SENSITIVITY_L']*CMTY_delta_kl['Rho_kl_H']
CMTY_delta_kl['rslt_kl_L']=CMTY_delta_kl['WEIGHTED_SENSITIVITY_K']*CMTY_delta_kl['WEIGHTED_SENSITIVITY_L']*CMTY_delta_kl['Rho_kl_L']


# In[301]:


CMTY_delta_kl.loc[(CMTY_delta_kl.RISK_FACTOR_ID_K==CMTY_delta_kl.RISK_FACTOR_ID_L)
                  &(CMTY_delta_kl.RISK_FACTOR_VERTEX_1_K==CMTY_delta_kl.RISK_FACTOR_VERTEX_1_L)
                  &(CMTY_delta_kl.COMM_LOCATION_K==CMTY_delta_kl.COMM_LOCATION_L),'rhol_M']=0
CMTY_delta_kl.loc[(CMTY_delta_kl.RISK_FACTOR_ID_K==CMTY_delta_kl.RISK_FACTOR_ID_L)
                  &(CMTY_delta_kl.RISK_FACTOR_VERTEX_1_K==CMTY_delta_kl.RISK_FACTOR_VERTEX_1_L)
                  &(CMTY_delta_kl.COMM_LOCATION_K==CMTY_delta_kl.COMM_LOCATION_L),'rhol_H']=0
CMTY_delta_kl.loc[(CMTY_delta_kl.RISK_FACTOR_ID_K==CMTY_delta_kl.RISK_FACTOR_ID_L)
                  &(CMTY_delta_kl.RISK_FACTOR_VERTEX_1_K==CMTY_delta_kl.RISK_FACTOR_VERTEX_1_L)
                  &(CMTY_delta_kl.COMM_LOCATION_K==CMTY_delta_kl.COMM_LOCATION_L),'rhol_L']=0

CMTY_delta_kl.loc[(CMTY_delta_kl.RISK_FACTOR_ID_K!=CMTY_delta_kl.RISK_FACTOR_ID_L)
                  |(CMTY_delta_kl.RISK_FACTOR_VERTEX_1_K!=CMTY_delta_kl.RISK_FACTOR_VERTEX_1_L)
                  |(CMTY_delta_kl.COMM_LOCATION_K!=CMTY_delta_kl.COMM_LOCATION_L),'rhol_M']=CMTY_delta_kl['WEIGHTED_SENSITIVITY_L']*CMTY_delta_kl['Rho_kl_M']
CMTY_delta_kl.loc[(CMTY_delta_kl.RISK_FACTOR_ID_K!=CMTY_delta_kl.RISK_FACTOR_ID_L)
                  |(CMTY_delta_kl.RISK_FACTOR_VERTEX_1_K!=CMTY_delta_kl.RISK_FACTOR_VERTEX_1_L)
                  |(CMTY_delta_kl.COMM_LOCATION_K!=CMTY_delta_kl.COMM_LOCATION_L),'rhol_H']=CMTY_delta_kl['WEIGHTED_SENSITIVITY_L']*CMTY_delta_kl['Rho_kl_H']
CMTY_delta_kl.loc[(CMTY_delta_kl.RISK_FACTOR_ID_K!=CMTY_delta_kl.RISK_FACTOR_ID_L)
                  |(CMTY_delta_kl.RISK_FACTOR_VERTEX_1_K!=CMTY_delta_kl.RISK_FACTOR_VERTEX_1_L)
                  |(CMTY_delta_kl.COMM_LOCATION_K!=CMTY_delta_kl.COMM_LOCATION_L),'rhol_L']=CMTY_delta_kl['WEIGHTED_SENSITIVITY_L']*CMTY_delta_kl['Rho_kl_L']


# In[302]:


CMTY_delta_agg=CMTY_delta.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()


# In[303]:


CMTY_delta_bc=CMTY_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                                   ).merge(CMTY_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c'
                                                                  ,'WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                                           ,on =['RISK_FACTOR_CLASS'],how='left')


# In[304]:


CMTY_delta_bc=CMTY_delta_bc.merge(CMTY_Gamma,on=['Bucket_b','Bucket_c'],how='left').rename({'Gamma_bc':'Gamma_bc_M'},axis=1)
CMTY_delta_bc['Gamma_bc_H']=np.minimum(1,High_Multipler*CMTY_delta_bc['Gamma_bc_M'])
CMTY_delta_bc['Gamma_bc_L']=np.maximum(Low_Multipler1*CMTY_delta_bc['Gamma_bc_M']-1,Low_Multipler2*CMTY_delta_bc['Gamma_bc_M'])


# In[305]:


CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc_M']=0
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc_M']=CMTY_delta_bc['WS_b']*CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_M']
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc_H']=0
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc_H']=CMTY_delta_bc['WS_b']*CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_H']
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc_L']=0
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc_L']=CMTY_delta_bc['WS_b']*CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_L']


# In[306]:


CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'gammac_M']=0
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'gammac_M']=CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_M']
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'gammac_H']=0
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'gammac_H']=CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_H']
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'gammac_L']=0
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'gammac_L']=CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_L']


# In[307]:


CMTY_delta_agg=CMTY_delta_agg.merge(CMTY_delta_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']]
                                    ,on=['RISK_FACTOR_BUCKET'],how='left'
                                   ).groupby (['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY'],dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()


# In[308]:


CMTY_delta_agg['Sb_H']=CMTY_delta_agg['WEIGHTED_SENSITIVITY']
CMTY_delta_agg['Sb_L']=CMTY_delta_agg['WEIGHTED_SENSITIVITY']
CMTY_delta_agg['Kb_M']=np.sqrt(CMTY_delta_agg['rslt_kl_M'])
CMTY_delta_agg['Kb_H']=np.sqrt(CMTY_delta_agg['rslt_kl_H'])
CMTY_delta_agg['Kb_L']=np.sqrt(CMTY_delta_agg['rslt_kl_L'])
CMTY_delta_agg=CMTY_delta_agg.rename({'WEIGHTED_SENSITIVITY':'Sb_M','rslt_kl_M':'Kb_M^2','rslt_kl_H':'Kb_H^2','rslt_kl_L':'Kb_L^2'},axis=1)


# In[309]:


CMTY_delta_agg['Sb*_M']=np.maximum(np.minimum(CMTY_delta_agg['Kb_M'],CMTY_delta_agg['Sb_M']),-CMTY_delta_agg['Kb_M'])
CMTY_delta_agg['Sb*_H']=np.maximum(np.minimum(CMTY_delta_agg['Kb_H'],CMTY_delta_agg['Sb_H']),-CMTY_delta_agg['Kb_H'])
CMTY_delta_agg['Sb*_L']=np.maximum(np.minimum(CMTY_delta_agg['Kb_L'],CMTY_delta_agg['Sb_L']),-CMTY_delta_agg['Kb_L'])


# In[310]:


CMTY_delta_bc=CMTY_delta_bc.merge(
    CMTY_delta_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
    ,left_on=['Bucket_b'],right_on=['RISK_FACTOR_BUCKET'],how='left')

CMTY_delta_bc=CMTY_delta_bc.merge(
    CMTY_delta_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
    ,left_on=['Bucket_c'],right_on=['RISK_FACTOR_BUCKET'],how='left')

CMTY_delta_bc=CMTY_delta_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)


# In[311]:


CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc*_M']=0
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc*_M']=CMTY_delta_bc['Sb*_M']*CMTY_delta_bc['Sc*_M']*CMTY_delta_bc['Gamma_bc_M']
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc*_H']=0
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc*_H']=CMTY_delta_bc['Sb*_H']*CMTY_delta_bc['Sc*_H']*CMTY_delta_bc['Gamma_bc_H']
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc*_L']=0
CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc*_L']=CMTY_delta_bc['Sb*_L']*CMTY_delta_bc['Sc*_L']*CMTY_delta_bc['Gamma_bc_L']


# In[312]:


cmtyd = pd.DataFrame([],columns=['GROUPING','RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=['0'])


# In[313]:


cmtyd_M_est=sum(CMTY_delta_agg['Kb_M^2'])+sum(CMTY_delta_bc['rslt_bc_M'])
cmtyd_M_1=np.sqrt(sum(CMTY_delta_agg['Kb_M^2'])+sum(CMTY_delta_bc['rslt_bc_M']))
cmtyd_M_2=np.sqrt(sum(CMTY_delta_agg['Kb_M^2'])+sum(CMTY_delta_bc['rslt_bc*_M']))


# In[314]:


cmtyd_H_est=sum(CMTY_delta_agg['Kb_H^2'])+sum(CMTY_delta_bc['rslt_bc_H'])
cmtyd_H_1=np.sqrt(sum(CMTY_delta_agg['Kb_H^2'])+sum(CMTY_delta_bc['rslt_bc_H']))
cmtyd_H_2=np.sqrt(sum(CMTY_delta_agg['Kb_H^2'])+sum(CMTY_delta_bc['rslt_bc*_H']))


# In[315]:


cmtyd_L_est=sum(CMTY_delta_agg['Kb_L^2'])+sum(CMTY_delta_bc['rslt_bc_L'])
cmtyd_L_1=np.sqrt(sum(CMTY_delta_agg['Kb_L^2'])+sum(CMTY_delta_bc['rslt_bc_L']))
cmtyd_L_2=np.sqrt(sum(CMTY_delta_agg['Kb_L^2'])+sum(CMTY_delta_bc['rslt_bc*_L']))


# In[316]:


cmtyd['RISK_FACTOR_CLASS']='CMTY'
cmtyd['SENS_TYPE']='DELTA'
cmtyd['NORMAL']=np.where(cmtyd_M_est>=0,cmtyd_M_1,cmtyd_M_2)
cmtyd['HIGH']=np.where(cmtyd_H_est>=0,cmtyd_H_1,cmtyd_H_2)
cmtyd['LOW']=np.where(cmtyd_L_est>=0,cmtyd_L_1,cmtyd_L_2)


# In[317]:


cmtyd_1=CMTY_delta[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','COMM_LOCATION','WEIGHTED_SENSITIVITY']]


# In[318]:


cmtyd_2=CMTY_delta_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','COMM_LOCATION_K','RISK_FACTOR_BUCKET']
                      ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()


# In[319]:


cmtyd_3=CMTY_delta_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                      ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()


# In[320]:


cmtyd_4=CMTY_delta_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]


# In[321]:


cmtyd_decomp=cmtyd_1.merge(cmtyd_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','COMM_LOCATION','RISK_FACTOR_BUCKET']
                          ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','COMM_LOCATION_K','RISK_FACTOR_BUCKET'],how='left')\
.merge(cmtyd_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
.merge(cmtyd_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
.merge(cmtyd,on=['RISK_FACTOR_CLASS'],how='left')


# In[322]:


cmtyd_decomp=cmtyd_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','COMM_LOCATION_K','Bucket_b','GROUPING','SENS_TYPE'],axis=1)


# In[323]:


cmtyd_decomp['M_est']=cmtyd_M_est
cmtyd_decomp['H_est']=cmtyd_H_est
cmtyd_decomp['L_est']=cmtyd_L_est


# In[324]:


#case 1
cmtyd_decomp.loc[(cmtyd_decomp['M_est']>=0)&(cmtyd_decomp['Kb_M']>0),'pder_M']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_M']+cmtyd_decomp['gammac_M'])/cmtyd_decomp['NORMAL']

cmtyd_decomp.loc[(cmtyd_decomp['H_est']>=0)&(cmtyd_decomp['Kb_H']>0),'pder_H']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_H']+cmtyd_decomp['gammac_H'])/cmtyd_decomp['HIGH']

cmtyd_decomp.loc[(cmtyd_decomp['L_est']>=0)&(cmtyd_decomp['Kb_L']>0),'pder_L']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_L']+cmtyd_decomp['gammac_L'])/cmtyd_decomp['LOW']


# In[325]:


#case 2
cmtyd_decomp.loc[(cmtyd_decomp['M_est']>=0)&(cmtyd_decomp['Kb_M']==0),'pder_M']=cmtyd_decomp['gammac_M']/cmtyd_decomp['NORMAL']

cmtyd_decomp.loc[(cmtyd_decomp['H_est']>=0)&(cmtyd_decomp['Kb_H']==0),'pder_H']=cmtyd_decomp['gammac_H']/cmtyd_decomp['HIGH']

cmtyd_decomp.loc[(cmtyd_decomp['L_est']>=0)&(cmtyd_decomp['Kb_L']==0),'pder_L']=cmtyd_decomp['gammac_L']/cmtyd_decomp['LOW']


# In[326]:


#case 3
cmtyd_decomp.loc[(cmtyd_decomp['M_est']<0)&(cmtyd_decomp['Kb_M']>0)&(cmtyd_decomp['Sb*_M']==cmtyd_decomp['Kb_M']),'pder_M']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_M'])*(1+1/cmtyd_decomp['Kb_M']*cmtyd_decomp['gammac_M']))/cmtyd_decomp['NORMAL']

cmtyd_decomp.loc[(cmtyd_decomp['H_est']<0)&(cmtyd_decomp['Kb_H']>0)&(cmtyd_decomp['Sb*_H']==cmtyd_decomp['Kb_H']),'pder_H']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_H'])*(1+1/cmtyd_decomp['Kb_H']*cmtyd_decomp['gammac_H']))/cmtyd_decomp['HIGH']

cmtyd_decomp.loc[(cmtyd_decomp['L_est']<0)&(cmtyd_decomp['Kb_L']>0)&(cmtyd_decomp['Sb*_L']==cmtyd_decomp['Kb_L']),'pder_L']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_L'])*(1+1/cmtyd_decomp['Kb_L']*cmtyd_decomp['gammac_L']))/cmtyd_decomp['LOW']


# In[327]:


#case 4
cmtyd_decomp.loc[(cmtyd_decomp['M_est']<0)&(cmtyd_decomp['Kb_M']>0)&(cmtyd_decomp['Sb*_M']+cmtyd_decomp['Kb_M']==0),'pder_M']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_M'])*(1-1/cmtyd_decomp['Kb_M']*cmtyd_decomp['gammac_M']))/cmtyd_decomp['NORMAL']

cmtyd_decomp.loc[(cmtyd_decomp['H_est']<0)&(cmtyd_decomp['Kb_H']>0)&(cmtyd_decomp['Sb*_H']+cmtyd_decomp['Kb_H']==0),'pder_H']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_H'])*(1-1/cmtyd_decomp['Kb_H']*cmtyd_decomp['gammac_H']))/cmtyd_decomp['HIGH']

cmtyd_decomp.loc[(cmtyd_decomp['L_est']<0)&(cmtyd_decomp['Kb_L']>0)&(cmtyd_decomp['Sb*_L']+cmtyd_decomp['Kb_L']==0),'pder_L']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_L'])*(1-1/cmtyd_decomp['Kb_L']*cmtyd_decomp['gammac_L']))/cmtyd_decomp['LOW']


# In[328]:


#case 5
cmtyd_decomp.loc[(cmtyd_decomp['M_est']<0)&(cmtyd_decomp['Kb_M']>0)&(abs(cmtyd_decomp['Sb*_M'])!=abs(cmtyd_decomp['Kb_M'])),'pder_M']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_M']+cmtyd_decomp['gammac_M'])/cmtyd_decomp['NORMAL']

cmtyd_decomp.loc[(cmtyd_decomp['H_est']<0)&(cmtyd_decomp['Kb_H']>0)&(abs(cmtyd_decomp['Sb*_H'])!=abs(cmtyd_decomp['Kb_H'])),'pder_H']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_H']+cmtyd_decomp['gammac_H'])/cmtyd_decomp['HIGH']

cmtyd_decomp.loc[(cmtyd_decomp['L_est']<0)&(cmtyd_decomp['Kb_L']>0)&(abs(cmtyd_decomp['Sb*_L'])!=abs(cmtyd_decomp['Kb_L'])),'pder_L']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_L']+cmtyd_decomp['gammac_L'])/cmtyd_decomp['LOW']


# In[329]:


#case 6
cmtyd_decomp.loc[(cmtyd_decomp['M_est']<0)&(cmtyd_decomp['Kb_M']==0),'pder_M']=0

cmtyd_decomp.loc[(cmtyd_decomp['H_est']<0)&(cmtyd_decomp['Kb_H']==0),'pder_H']=0

cmtyd_decomp.loc[(cmtyd_decomp['L_est']<0)&(cmtyd_decomp['Kb_L']==0),'pder_L']=0


# In[330]:


cmtyd_decomp=cmtyd_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','COMM_LOCATION','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]


# In[331]:


cmtyd_decomp_rslt=CMTY_RawData[(CMTY_RawData.SENSITIVITY_TYPE=='Delta')].merge(cmtyd_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','COMM_LOCATION','RISK_FACTOR_BUCKET'],how='left')


# In[332]:


#sum(cmtyd_decomp_rslt['WEIGHTED_SENSITIVITY']*cmtyd_decomp_rslt['pder_M'])


# In[333]:


#sum(cmtyd_decomp_rslt['WEIGHTED_SENSITIVITY']*cmtyd_decomp_rslt['pder_H'])


# In[334]:


#sum(cmtyd_decomp_rslt['WEIGHTED_SENSITIVITY']*cmtyd_decomp_rslt['pder_L'])


# In[335]:


#cmtyd


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[336]:


CMTY_vega = CMTY_Position[(CMTY_Position['SENSITIVITY_TYPE']=='Vega')]


# In[337]:


CMTY_vega_kl = CMTY_vega.rename(
    {'RISK_FACTOR_ID':'RISK_FACTOR_ID_K'
     ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_K'
     ,'COMM_ASSET':'COMM_ASSET_K'
     ,'COMM_LOCATION':'COMM_LOCATION_K'
     ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_K'},axis=1
).merge(CMTY_vega[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','COMM_ASSET','COMM_LOCATION','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
        .rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_L'
                 ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_L'
                 ,'COMM_ASSET':'COMM_ASSET_L'
                 ,'COMM_LOCATION':'COMM_LOCATION_L'
                 ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_L'},axis=1)
        ,on=['RISK_FACTOR_BUCKET'],how='left')


# In[338]:


CMTY_delta_kl = CMTY_vega_kl.merge(CMTY_Rho_Cty,on='RISK_FACTOR_BUCKET',how='left')


# In[339]:


CMTY_vega_kl.loc[CMTY_vega_kl['COMM_ASSET_K']==CMTY_vega_kl['COMM_ASSET_L'],'Rho_Cty']=1
CMTY_vega_kl.loc[CMTY_vega_kl['COMM_ASSET_K']!=CMTY_vega_kl['COMM_ASSET_L'],'Rho_Cty']=CMTY_delta_kl['Rho']
CMTY_vega_kl.loc[CMTY_vega_kl['RISK_FACTOR_VERTEX_1_K']==CMTY_vega_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=1
CMTY_vega_kl.loc[CMTY_vega_kl['RISK_FACTOR_VERTEX_1_K']!=CMTY_vega_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=CMTY_Rho_Tenor
CMTY_vega_kl.loc[CMTY_vega_kl['RISK_FACTOR_ID_K']==CMTY_vega_kl['RISK_FACTOR_ID_L'],'Rho_Basis']=1
CMTY_vega_kl.loc[CMTY_vega_kl['RISK_FACTOR_ID_K']!=CMTY_vega_kl['RISK_FACTOR_ID_L'],'Rho_Basis']=CMTY_Rho_Basis


# In[340]:


CMTY_vega_kl['Rho_kl_delta_M'] = CMTY_vega_kl['Rho_Cty']*CMTY_vega_kl['Rho_Tenor']*CMTY_vega_kl['Rho_Basis']
CMTY_vega_kl['Rho_kl_opt_mat_M'] = np.exp(
    -0.01*abs(
        CMTY_vega_kl['RISK_FACTOR_VERTEX_1_K']-CMTY_vega_kl['RISK_FACTOR_VERTEX_1_L']
    )/np.minimum(CMTY_vega_kl['RISK_FACTOR_VERTEX_1_K'],CMTY_vega_kl['RISK_FACTOR_VERTEX_1_L']))
CMTY_vega_kl['Rho_kl_M']=np.minimum((CMTY_vega_kl['Rho_kl_opt_mat_M']*CMTY_vega_kl['Rho_kl_delta_M']),1)


# In[341]:


CMTY_vega_kl['Rho_kl_M']=CMTY_vega_kl['Rho_kl_delta_M']
CMTY_vega_kl['Rho_kl_H']=np.minimum(1,High_Multipler*CMTY_vega_kl['Rho_kl_M'])
CMTY_vega_kl['Rho_kl_L']=np.maximum(Low_Multipler1*CMTY_vega_kl['Rho_kl_M']-1,Low_Multipler2*CMTY_vega_kl['Rho_kl_M'])
CMTY_vega_kl['rslt_kl_M']=CMTY_vega_kl['Rho_kl_M']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_K']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_L']
CMTY_vega_kl['rslt_kl_H']=CMTY_vega_kl['Rho_kl_H']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_K']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_L']
CMTY_vega_kl['rslt_kl_L']=CMTY_vega_kl['Rho_kl_L']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_K']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_L']


# In[342]:


CMTY_vega_kl.loc[(CMTY_vega_kl.RISK_FACTOR_ID_K==CMTY_vega_kl.RISK_FACTOR_ID_L)
                 &(CMTY_vega_kl.RISK_FACTOR_VERTEX_1_K==CMTY_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=0
CMTY_vega_kl.loc[(CMTY_vega_kl.RISK_FACTOR_ID_K==CMTY_vega_kl.RISK_FACTOR_ID_L)
                 &(CMTY_vega_kl.RISK_FACTOR_VERTEX_1_K==CMTY_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=0
CMTY_vega_kl.loc[(CMTY_vega_kl.RISK_FACTOR_ID_K==CMTY_vega_kl.RISK_FACTOR_ID_L)
                 &(CMTY_vega_kl.RISK_FACTOR_VERTEX_1_K==CMTY_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=0

CMTY_vega_kl.loc[(CMTY_vega_kl.RISK_FACTOR_ID_K!=CMTY_vega_kl.RISK_FACTOR_ID_L)
                 |(CMTY_vega_kl.RISK_FACTOR_VERTEX_1_K!=CMTY_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=CMTY_vega_kl['WEIGHTED_SENSITIVITY_L']*CMTY_vega_kl['Rho_kl_M']
CMTY_vega_kl.loc[(CMTY_vega_kl.RISK_FACTOR_ID_K!=CMTY_vega_kl.RISK_FACTOR_ID_L)
                 |(CMTY_vega_kl.RISK_FACTOR_VERTEX_1_K!=CMTY_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=CMTY_vega_kl['WEIGHTED_SENSITIVITY_L']*CMTY_vega_kl['Rho_kl_H']
CMTY_vega_kl.loc[(CMTY_vega_kl.RISK_FACTOR_ID_K!=CMTY_vega_kl.RISK_FACTOR_ID_L)
                 |(CMTY_vega_kl.RISK_FACTOR_VERTEX_1_K!=CMTY_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=CMTY_vega_kl['WEIGHTED_SENSITIVITY_L']*CMTY_vega_kl['Rho_kl_L']


# In[343]:


CMTY_vega_agg = CMTY_vega.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()


# In[344]:


CMTY_vega_bc = CMTY_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
               ).merge(CMTY_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c','WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                       ,on=['RISK_FACTOR_CLASS'],how='left')


# In[345]:


CMTY_vega_bc = CMTY_vega_bc.merge(CMTY_Gamma,on=['Bucket_b','Bucket_c'],how='left').rename({'Gamma_bc':'Gamma_bc_M'},axis=1)
CMTY_vega_bc['Gamma_bc_H'] = np.minimum(1, CMTY_vega_bc['Gamma_bc_M']*High_Multipler)
CMTY_vega_bc['Gamma_bc_L'] = np.maximum((Low_Multipler1*CMTY_vega_bc['Gamma_bc_M']-1),(Low_Multipler2*CMTY_vega_bc['Gamma_bc_M']))


# In[346]:


CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_M']==1,'rslt_bc_M']=0
CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_M']!=1,'rslt_bc_M']=CMTY_vega_bc.WS_b*CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_M
CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_H']==1,'rslt_bc_H']=0
CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_H']!=1,'rslt_bc_H']=CMTY_vega_bc.WS_b*CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_H
CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_L']==1,'rslt_bc_L']=0
CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_L']!=1,'rslt_bc_L']=CMTY_vega_bc.WS_b*CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_L


# In[347]:


CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']==CMTY_vega_bc['Bucket_c'],'gammac_M']=0
CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']!=CMTY_vega_bc['Bucket_c'],'gammac_M']=CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_M
CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']==CMTY_vega_bc['Bucket_c'],'gammac_H']=0
CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']!=CMTY_vega_bc['Bucket_c'],'gammac_H']=CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_H
CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']==CMTY_vega_bc['Bucket_c'],'gammac_L']=0
CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']!=CMTY_vega_bc['Bucket_c'],'gammac_L']=CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_L


# In[348]:


CMTY_vega_agg=CMTY_vega_agg.merge(
    CMTY_vega_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']],on=['RISK_FACTOR_BUCKET'],how='left'
).groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']
                                      ,dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()
CMTY_vega_agg['Sb_H']=CMTY_vega_agg['WEIGHTED_SENSITIVITY']
CMTY_vega_agg['Sb_L']=CMTY_vega_agg['WEIGHTED_SENSITIVITY']
CMTY_vega_agg['Kb_M']=np.sqrt(CMTY_vega_agg['rslt_kl_M'])
CMTY_vega_agg['Kb_H']=np.sqrt(CMTY_vega_agg['rslt_kl_H'])
CMTY_vega_agg['Kb_L']=np.sqrt(CMTY_vega_agg['rslt_kl_L'])
CMTY_vega_agg = CMTY_vega_agg.rename({'WEIGHTED_SENSITIVITY':'Sb_M','rslt_kl_M':'Kb_M^2','rslt_kl_H':'Kb_H^2','rslt_kl_L':'Kb_L^2'},axis=1)


# In[349]:


CMTY_vega_agg['Sb*_M']=np.maximum(np.minimum(CMTY_vega_agg['Kb_M'],CMTY_vega_agg['Sb_M']),-CMTY_vega_agg['Kb_M'])
CMTY_vega_agg['Sb*_H']=np.maximum(np.minimum(CMTY_vega_agg['Kb_H'],CMTY_vega_agg['Sb_H']),-CMTY_vega_agg['Kb_H'])
CMTY_vega_agg['Sb*_L']=np.maximum(np.minimum(CMTY_vega_agg['Kb_L'],CMTY_vega_agg['Sb_L']),-CMTY_vega_agg['Kb_L'])


# In[350]:


CMTY_vega_bc=CMTY_vega_bc.merge(
    CMTY_vega_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
    ,left_on=['Bucket_b'],right_on=['RISK_FACTOR_BUCKET'],how='left')

CMTY_vega_bc=CMTY_vega_bc.merge(
    CMTY_vega_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
    ,left_on=['Bucket_c'],right_on=['RISK_FACTOR_BUCKET'],how='left')

CMTY_vega_bc=CMTY_vega_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)


# In[351]:


CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_M']==1,'rslt_bc*_M']=0
CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_M']!=1,'rslt_bc*_M']=CMTY_vega_bc['Sb*_M']*CMTY_vega_bc['Sc*_M']*CMTY_vega_bc['Gamma_bc_M']
CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_H']==1,'rslt_bc*_H']=0
CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_H']!=1,'rslt_bc*_H']=CMTY_vega_bc['Sb*_H']*CMTY_vega_bc['Sc*_H']*CMTY_vega_bc['Gamma_bc_H']
CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_L']==1,'rslt_bc*_L']=0
CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_L']!=1,'rslt_bc*_L']=CMTY_vega_bc['Sb*_L']*CMTY_vega_bc['Sc*_L']*CMTY_vega_bc['Gamma_bc_L']


# In[ ]:





# In[352]:


cmtyv=pd.DataFrame([],columns=['GROUPING','RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])


# In[353]:


cmtyv_M_est=sum(CMTY_vega_agg['Kb_M^2'])+sum(CMTY_vega_bc['rslt_bc_M'])
cmtyv_M_1=np.sqrt(sum(CMTY_vega_agg['Kb_M^2'])+sum(CMTY_vega_bc['rslt_bc_M']))
cmtyv_M_2=np.sqrt(sum(CMTY_vega_agg['Kb_M^2'])+sum(CMTY_vega_bc['rslt_bc*_M']))


# In[354]:


cmtyv_H_est=sum(CMTY_vega_agg['Kb_H^2'])+sum(CMTY_vega_bc['rslt_bc_H'])
cmtyv_H_1=np.sqrt(sum(CMTY_vega_agg['Kb_H^2'])+sum(CMTY_vega_bc['rslt_bc_H']))
cmtyv_H_2=np.sqrt(sum(CMTY_vega_agg['Kb_H^2'])+sum(CMTY_vega_bc['rslt_bc*_H']))


# In[355]:


cmtyv_L_est=sum(CMTY_vega_agg['Kb_L^2'])+sum(CMTY_vega_bc['rslt_bc_L'])
cmtyv_L_1=np.sqrt(sum(CMTY_vega_agg['Kb_L^2'])+sum(CMTY_vega_bc['rslt_bc_L']))
cmtyv_L_2=np.sqrt(sum(CMTY_vega_agg['Kb_L^2'])+sum(CMTY_vega_bc['rslt_bc*_L']))


# In[356]:


cmtyv['RISK_FACTOR_CLASS']='CMTY'
cmtyv['SENS_TYPE']='VEGA'
cmtyv['NORMAL']=np.where(cmtyv_M_est>=0,cmtyv_M_1,cmtyv_M_2)
cmtyv['HIGH']=np.where(cmtyv_H_est>=0,cmtyv_H_1,cmtyv_H_2)
cmtyv['LOW']=np.where(cmtyv_L_est>=0,cmtyv_L_1,cmtyv_L_2)


# In[357]:


cmtyv_1=CMTY_vega[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]


# In[358]:


cmtyv_2=CMTY_vega_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                             ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()


# In[359]:


cmtyv_3=CMTY_vega_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                             ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()


# In[360]:


cmtyv_4=CMTY_vega_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]


# In[361]:


cmtyv_decomp=cmtyv_1.merge(cmtyv_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET']
                           ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                           ,how='left')\
.merge(cmtyv_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
.merge(cmtyv_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
.merge(cmtyv,on=['RISK_FACTOR_CLASS'],how='left')


# In[362]:


cmtyv_decomp=cmtyv_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','Bucket_b','GROUPING','SENS_TYPE'],axis=1)


# In[363]:


cmtyv_decomp['M_est']=cmtyv_M_est
cmtyv_decomp['H_est']=cmtyv_H_est
cmtyv_decomp['L_est']=cmtyv_L_est


# In[364]:


#case 1
cmtyv_decomp.loc[(cmtyv_decomp['M_est']>=0)&(cmtyv_decomp['Kb_M']>0),'pder_M']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_M']+cmtyv_decomp['gammac_M'])/cmtyv_decomp['NORMAL']

cmtyv_decomp.loc[(cmtyv_decomp['H_est']>=0)&(cmtyv_decomp['Kb_H']>0),'pder_H']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_H']+cmtyv_decomp['gammac_H'])/cmtyv_decomp['HIGH']

cmtyv_decomp.loc[(cmtyv_decomp['L_est']>=0)&(cmtyv_decomp['Kb_L']>0),'pder_L']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_L']+cmtyv_decomp['gammac_L'])/cmtyv_decomp['LOW']


# In[365]:


#case 2
cmtyv_decomp.loc[(cmtyv_decomp['M_est']>=0)&(cmtyv_decomp['Kb_M']==0),'pder_M']=cmtyv_decomp['gammac_M']/cmtyv_decomp['NORMAL']

cmtyv_decomp.loc[(cmtyv_decomp['H_est']>=0)&(cmtyv_decomp['Kb_H']==0),'pder_H']=cmtyv_decomp['gammac_H']/cmtyv_decomp['HIGH']

cmtyv_decomp.loc[(cmtyv_decomp['L_est']>=0)&(cmtyv_decomp['Kb_L']==0),'pder_L']=cmtyv_decomp['gammac_L']/cmtyv_decomp['LOW']


# In[366]:


#case 3
cmtyv_decomp.loc[(cmtyv_decomp['M_est']<0)&(cmtyv_decomp['Kb_M']>0)&(cmtyv_decomp['Sb*_M']==cmtyv_decomp['Kb_M']),'pder_M']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_M'])*(1+1/cmtyv_decomp['Kb_M']*cmtyv_decomp['gammac_M']))/cmtyv_decomp['NORMAL']

cmtyv_decomp.loc[(cmtyv_decomp['H_est']<0)&(cmtyv_decomp['Kb_H']>0)&(cmtyv_decomp['Sb*_H']==cmtyv_decomp['Kb_H']),'pder_H']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_H'])*(1+1/cmtyv_decomp['Kb_H']*cmtyv_decomp['gammac_H']))/cmtyv_decomp['HIGH']

cmtyv_decomp.loc[(cmtyv_decomp['L_est']<0)&(cmtyv_decomp['Kb_L']>0)&(cmtyv_decomp['Sb*_L']==cmtyv_decomp['Kb_L']),'pder_L']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_L'])*(1+1/cmtyv_decomp['Kb_L']*cmtyv_decomp['gammac_L']))/cmtyv_decomp['LOW']


# In[367]:


#case 4
cmtyv_decomp.loc[(cmtyv_decomp['M_est']<0)&(cmtyv_decomp['Kb_M']>0)&(cmtyv_decomp['Sb*_M']+cmtyv_decomp['Kb_M']==0),'pder_M']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_M'])*(1-1/cmtyv_decomp['Kb_M']*cmtyv_decomp['gammac_M']))/cmtyv_decomp['NORMAL']

cmtyv_decomp.loc[(cmtyv_decomp['H_est']<0)&(cmtyv_decomp['Kb_H']>0)&(cmtyv_decomp['Sb*_H']+cmtyv_decomp['Kb_H']==0),'pder_H']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_H'])*(1-1/cmtyv_decomp['Kb_H']*cmtyv_decomp['gammac_H']))/cmtyv_decomp['HIGH']

cmtyv_decomp.loc[(cmtyv_decomp['L_est']<0)&(cmtyv_decomp['Kb_L']>0)&(cmtyv_decomp['Sb*_L']+cmtyv_decomp['Kb_L']==0),'pder_L']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_L'])*(1-1/cmtyv_decomp['Kb_L']*cmtyv_decomp['gammac_L']))/cmtyv_decomp['LOW']


# In[368]:


#case 5
cmtyv_decomp.loc[(cmtyv_decomp['M_est']<0)&(cmtyv_decomp['Kb_M']>0)&(abs(cmtyv_decomp['Sb*_M'])!=abs(cmtyv_decomp['Kb_M'])),'pder_M']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_M']+cmtyv_decomp['gammac_M'])/cmtyv_decomp['NORMAL']

cmtyv_decomp.loc[(cmtyv_decomp['H_est']<0)&(cmtyv_decomp['Kb_H']>0)&(abs(cmtyv_decomp['Sb*_H'])!=abs(cmtyv_decomp['Kb_H'])),'pder_H']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_H']+cmtyv_decomp['gammac_H'])/cmtyv_decomp['HIGH']

cmtyv_decomp.loc[(cmtyv_decomp['L_est']<0)&(cmtyv_decomp['Kb_L']>0)&(abs(cmtyv_decomp['Sb*_L'])!=abs(cmtyv_decomp['Kb_L'])),'pder_L']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_L']+cmtyv_decomp['gammac_L'])/cmtyv_decomp['LOW']


# In[369]:


#case 6
cmtyv_decomp.loc[(cmtyv_decomp['M_est']<0)&(cmtyv_decomp['Kb_M']==0),'pder_M']=0

cmtyv_decomp.loc[(cmtyv_decomp['H_est']<0)&(cmtyv_decomp['Kb_H']==0),'pder_H']=0

cmtyv_decomp.loc[(cmtyv_decomp['L_est']<0)&(cmtyv_decomp['Kb_L']==0),'pder_L']=0


# In[370]:


cmtyv_decomp=cmtyv_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]


# In[371]:


cmtyv_decomp_rslt=CMTY_RawData[(CMTY_RawData.SENSITIVITY_TYPE=='Vega')].merge(cmtyv_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'],how='left')


# In[372]:


cmtyv_decomp_rslt=cmtyv_decomp_rslt.fillna({'pder_M':0,'pder_H':0,'pder_L':0})


# In[373]:


#sum(cmtyv_decomp_rslt['WEIGHTED_SENSITIVITY']*cmtyv_decomp_rslt['pder_M'])


# In[374]:


#sum(cmtyv_decomp_rslt['WEIGHTED_SENSITIVITY']*cmtyv_decomp_rslt['pder_H'])


# In[375]:


#sum(cmtyv_decomp_rslt['WEIGHTED_SENSITIVITY']*cmtyv_decomp_rslt['pder_L'])


# In[376]:


#cmtyv


# In[ ]:





# In[ ]:





# In[377]:


CMTY_curvature = CMTY_Position.query('SENSITIVITY_TYPE=="Curvature Up"|SENSITIVITY_TYPE=="Curvature Down"')


# In[378]:


CMTY_curvature = CMTY_curvature.groupby(['RISK_FACTOR_ID','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE']
                                      ,dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()


# In[379]:


CMTY_curvature = CMTY_curvature.assign(max_0_square=np.square(np.maximum(CMTY_curvature['WEIGHTED_SENSITIVITY'],0)))


# In[380]:


CMTY_curvature_agg = CMTY_curvature.groupby(
    ['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],dropna=False
).agg({'WEIGHTED_SENSITIVITY':'sum','max_0_square':'sum'}).reset_index()


# In[381]:


CMTY_curvature_agg['max_0_k']=np.sqrt(CMTY_curvature_agg['max_0_square'])


# In[382]:


CMTY_curvature_agg=CMTY_curvature_agg.pivot(index=('RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET')
                         ,columns='SENSITIVITY_TYPE')


# In[383]:


CMTY_curvature_agg.columns=['/'.join(i) for i in CMTY_curvature_agg.columns]
CMTY_curvature_agg=CMTY_curvature_agg.reset_index()


# In[384]:


CMTY_curvature_agg['Kb+_M']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Up'])))
CMTY_curvature_agg['Kb-_M']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Down'])))
CMTY_curvature_agg['Kb_M']=np.maximum(CMTY_curvature_agg['Kb+_M'],CMTY_curvature_agg['Kb-_M'])
CMTY_curvature_agg['Kb_M^2']=np.square(CMTY_curvature_agg['Kb_M'])
CMTY_curvature_agg['Sb_M']=np.select([(CMTY_curvature_agg['Kb_M'] == CMTY_curvature_agg['Kb+_M']),
                                      (CMTY_curvature_agg['Kb_M'] != CMTY_curvature_agg['Kb+_M'])],
                                     [(CMTY_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Up']),
                                      (CMTY_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Down'])])


# In[385]:


CMTY_curvature_agg['Kb+_H']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Up'])))
CMTY_curvature_agg['Kb-_H']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Down'])))
CMTY_curvature_agg['Kb_H']=np.maximum(CMTY_curvature_agg['Kb+_H'],CMTY_curvature_agg['Kb-_H'])
CMTY_curvature_agg['Kb_H^2']=np.square(CMTY_curvature_agg['Kb_H'])
CMTY_curvature_agg['Sb_H']=np.select([(CMTY_curvature_agg['Kb_H'] == CMTY_curvature_agg['Kb+_H']),
                                      (CMTY_curvature_agg['Kb_H'] != CMTY_curvature_agg['Kb+_H'])],
                                     [(CMTY_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Up']),
                                      (CMTY_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Down'])])


# In[386]:


CMTY_curvature_agg['Kb+_L']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Up'])))
CMTY_curvature_agg['Kb-_L']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Down'])))
CMTY_curvature_agg['Kb_L']=np.maximum(CMTY_curvature_agg['Kb+_L'],CMTY_curvature_agg['Kb-_L'])
CMTY_curvature_agg['Kb_L^2']=np.square(CMTY_curvature_agg['Kb_L'])
CMTY_curvature_agg['Sb_L']=np.select([(CMTY_curvature_agg['Kb_L'] == CMTY_curvature_agg['Kb+_L']),
                                      (CMTY_curvature_agg['Kb_L'] != CMTY_curvature_agg['Kb+_L'])],
                                     [(CMTY_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Up']),
                                      (CMTY_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Down'])])


# In[387]:


CMTY_curvature_agg['max']=np.select([(CMTY_curvature_agg['Kb_M'] == CMTY_curvature_agg['Kb+_M']),
                                     (CMTY_curvature_agg['Kb_M'] != CMTY_curvature_agg['Kb+_M'])],
                                    [(CMTY_curvature_agg['max_0_k/Curvature Up']),
                                     (CMTY_curvature_agg['max_0_k/Curvature Down'])])


# In[388]:


CMTY_curvature_agg['sign']=np.select([(CMTY_curvature_agg['Kb_M'] == CMTY_curvature_agg['Kb+_M']),
                                      (CMTY_curvature_agg['Kb_M'] != CMTY_curvature_agg['Kb+_M'])],
                                     ['Curvature Up','Curvature Down'])


# In[389]:


CMTY_curvature_bc=CMTY_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Sb_M']]
CMTY_curvature_bc=CMTY_curvature_bc.rename(
    {'Sb_M':'Sb','RISK_FACTOR_BUCKET':'Bucket_b'},axis=1
).merge(CMTY_curvature_bc.rename(
    {'Sb_M':'Sc','RISK_FACTOR_BUCKET':'Bucket_c'},axis=1
),on=['RISK_FACTOR_CLASS'],how='left')


# In[390]:


CMTY_curvature_bc.loc[(CMTY_curvature_bc['Sb']<0) & (CMTY_curvature_bc['Sc']<0),'Psi']=0
CMTY_curvature_bc.loc[(CMTY_curvature_bc['Sb']>=0) | (CMTY_curvature_bc['Sc']>=0),'Psi']=1
CMTY_curvature_bc=CMTY_curvature_bc.merge(CMTY_Gamma,on=['Bucket_b','Bucket_c'],how='left')
CMTY_curvature_bc['Gamma_bc_M']=np.square(CMTY_curvature_bc['Gamma_bc'])


# In[391]:



CMTY_curvature_bc['Gamma_bc_H']=np.square(np.minimum(1,CMTY_curvature_bc['Gamma_bc']*High_Multipler))
CMTY_curvature_bc['Gamma_bc_L']=np.square(np.maximum((Low_Multipler1*CMTY_curvature_bc['Gamma_bc']-1),(Low_Multipler2*CMTY_curvature_bc['Gamma_bc'])))
CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'rslt_bc_M']=0
CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'rslt_bc_M']=CMTY_curvature_bc['Gamma_bc_M']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sb']*CMTY_curvature_bc['Sc']
CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'rslt_bc_H']=0
CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'rslt_bc_H']=CMTY_curvature_bc['Gamma_bc_H']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sb']*CMTY_curvature_bc['Sc']
CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'rslt_bc_L']=0
CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'rslt_bc_L']=CMTY_curvature_bc['Gamma_bc_L']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sb']*CMTY_curvature_bc['Sc']


# In[392]:


CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'gammac_M']=0
CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'gammac_M']=CMTY_curvature_bc['Gamma_bc_M']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sc']
CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'gammac_H']=0
CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'gammac_H']=CMTY_curvature_bc['Gamma_bc_H']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sc']
CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'gammac_L']=0
CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'gammac_L']=CMTY_curvature_bc['Gamma_bc_L']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sc']


# In[393]:


cmtyc_M_est=sum(CMTY_curvature_agg['Kb_M^2'])+sum(CMTY_curvature_bc['rslt_bc_M'])
cmtyc_H_est=sum(CMTY_curvature_agg['Kb_H^2'])+sum(CMTY_curvature_bc['rslt_bc_H'])
cmtyc_L_est=sum(CMTY_curvature_agg['Kb_L^2'])+sum(CMTY_curvature_bc['rslt_bc_L'])


# In[394]:


cmtyc_M = np.sqrt(np.maximum(0,sum(CMTY_curvature_agg['Kb_M^2'])+sum(CMTY_curvature_bc['rslt_bc_M'])))
cmtyc_H = np.sqrt(np.maximum(0,sum(CMTY_curvature_agg['Kb_H^2'])+sum(CMTY_curvature_bc['rslt_bc_H'])))
cmtyc_L = np.sqrt(np.maximum(0,sum(CMTY_curvature_agg['Kb_L^2'])+sum(CMTY_curvature_bc['rslt_bc_L'])))


# In[395]:


cmtyc=pd.DataFrame([],columns=['GROUPING','RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])


# In[396]:


cmtyc['RISK_FACTOR_CLASS']='CMTY'
cmtyc['SENS_TYPE']='CURVATURE'
cmtyc['NORMAL']=cmtyc_M
cmtyc['HIGH']=cmtyc_H
cmtyc['LOW']=cmtyc_L


# In[397]:


cmtyc_1=CMTY_curvature[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]


# In[398]:


cmtyc_3=CMTY_curvature_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                                  ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()


# In[399]:


cmtyc_4=CMTY_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','max','sign']]


# In[400]:


cmtyc_decomp=cmtyc_1.merge(cmtyc_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET']
                           ,right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
.merge(cmtyc_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
.merge(cmtyc,on=['RISK_FACTOR_CLASS'],how='left')


# In[401]:


cmtyc_decomp=cmtyc_decomp.drop(['Bucket_b','GROUPING','SENS_TYPE'],axis=1)


# In[402]:


cmtyc_decomp['M_est']=cmtyc_M_est
cmtyc_decomp['H_est']=cmtyc_H_est
cmtyc_decomp['L_est']=cmtyc_L_est


# In[403]:


cmtyc_decomp=cmtyc_decomp[(cmtyc_decomp.SENSITIVITY_TYPE==cmtyc_decomp.sign)]


# In[404]:


#case 1/2
cmtyc_decomp.loc[(cmtyc_decomp['M_est']>=0),'pder_M']=(cmtyc_decomp['max']+cmtyc_decomp['gammac_M'])/cmtyc_decomp['NORMAL']

cmtyc_decomp.loc[(cmtyc_decomp['H_est']>=0),'pder_H']=(cmtyc_decomp['max']+cmtyc_decomp['gammac_H'])/cmtyc_decomp['HIGH']

cmtyc_decomp.loc[(cmtyc_decomp['L_est']>=0),'pder_L']=(cmtyc_decomp['max']+cmtyc_decomp['gammac_L'])/cmtyc_decomp['LOW']


# In[405]:


#case 3 
cmtyc_decomp.loc[(cmtyc_decomp['M_est']<0),'pder_M']=0
cmtyc_decomp.loc[(cmtyc_decomp['H_est']<0),'pder_H']=0
cmtyc_decomp.loc[(cmtyc_decomp['L_est']<0),'pder_L']=0


# In[406]:


cmtyc_decomp=cmtyc_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','pder_M','pder_H','pder_L']]


# In[407]:


cmtyc_decomp_rslt=CMTY_RawData.query('SENSITIVITY_TYPE=="Curvature Up"|SENSITIVITY_TYPE=="Curvature Down"').merge(cmtyc_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],how='right')


# In[408]:


#sum(cmtyc_decomp_rslt['WEIGHTED_SENSITIVITY']*cmtyc_decomp_rslt['pder_M'])


# In[409]:


#sum(cmtyc_decomp_rslt['WEIGHTED_SENSITIVITY']*cmtyc_decomp_rslt['pder_H'])


# In[410]:


#sum(cmtyc_decomp_rslt['WEIGHTED_SENSITIVITY']*cmtyc_decomp_rslt['pder_L'])


# In[411]:


#cmtyc


# In[ ]:





# In[ ]:





# In[412]:


FX_Weights = params.parse('FX_Weights')
FX_Gamma = 0.6
FX_LH = 40
FX_vega_rw = 1


# In[413]:


FX_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='FX')]

FX_Position = FX_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                          'RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]

FX_Position = FX_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                   'RISK_FACTOR_BUCKET','SENSITIVITY_TYPE']
                                  ,dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()


# In[414]:


FX_delta = FX_Position[(FX_Position['SENSITIVITY_TYPE']=='Delta')]


# In[415]:


FX_delta_agg=FX_delta.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()


# In[416]:


FX_delta_bc=FX_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                                   ).merge(FX_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c'
                                                                  ,'WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                                           ,on =['RISK_FACTOR_CLASS'],how='left')
FX_delta_bc = FX_delta_bc.loc[(FX_delta_bc.Bucket_b!=FX_delta_bc.Bucket_c),:].reset_index(drop=True)
FX_delta_bc['Gamma_bc_M']=FX_Gamma
FX_delta_bc['Gamma_bc_H']=np.minimum(1,High_Multipler*FX_delta_bc['Gamma_bc_M'])
FX_delta_bc['Gamma_bc_L']=np.maximum(Low_Multipler1*FX_delta_bc['Gamma_bc_M']-1,Low_Multipler2*FX_delta_bc['Gamma_bc_M'])


# In[417]:


FX_delta_bc['rslt_bc_M']=FX_delta_bc['WS_b']*FX_delta_bc['WS_c']*FX_delta_bc['Gamma_bc_M']
FX_delta_bc['rslt_bc_H']=FX_delta_bc['WS_b']*FX_delta_bc['WS_c']*FX_delta_bc['Gamma_bc_H']
FX_delta_bc['rslt_bc_L']=FX_delta_bc['WS_b']*FX_delta_bc['WS_c']*FX_delta_bc['Gamma_bc_L']


# In[418]:


FX_delta_bc['gammac_M']=FX_delta_bc.WS_c*FX_delta_bc.Gamma_bc_M
FX_delta_bc['gammac_H']=FX_delta_bc.WS_c*FX_delta_bc.Gamma_bc_H
FX_delta_bc['gammac_L']=FX_delta_bc.WS_c*FX_delta_bc.Gamma_bc_L


# In[419]:


FX_delta_agg['Sb_M']=FX_delta_agg['Sb_H']=FX_delta_agg['Sb_L']=FX_delta_agg['WEIGHTED_SENSITIVITY']
FX_delta_agg['Kb_M']=FX_delta_agg['Kb_H']=FX_delta_agg['Kb_L']=np.sqrt(np.maximum(0,np.square(FX_delta_agg['WEIGHTED_SENSITIVITY'])))
FX_delta_agg['Kb_M^2']=np.square(FX_delta_agg['Kb_M'])
FX_delta_agg['Kb_H^2']=np.square(FX_delta_agg['Kb_H'])
FX_delta_agg['Kb_L^2']=np.square(FX_delta_agg['Kb_L'])


# In[420]:


FX_delta_agg['Sb*_M']=np.maximum(np.minimum(FX_delta_agg['Kb_M'],FX_delta_agg['Sb_M']),-FX_delta_agg['Kb_M'])
FX_delta_agg['Sb*_H']=np.maximum(np.minimum(FX_delta_agg['Kb_H'],FX_delta_agg['Sb_H']),-FX_delta_agg['Kb_H'])
FX_delta_agg['Sb*_L']=np.maximum(np.minimum(FX_delta_agg['Kb_L'],FX_delta_agg['Sb_L']),-FX_delta_agg['Kb_L'])

FX_delta_bc=FX_delta_bc.merge(
    FX_delta_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
    ,left_on=['Bucket_b'],right_on=['RISK_FACTOR_BUCKET'],how='left')

FX_delta_bc=FX_delta_bc.merge(
    FX_delta_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
    ,left_on=['Bucket_c'],right_on=['RISK_FACTOR_BUCKET'],how='left')

FX_delta_bc=FX_delta_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)


# In[421]:


FX_delta_bc['rslt_bc*_M']=FX_delta_bc['Sb*_M']*FX_delta_bc['Sc*_M']*FX_delta_bc['Gamma_bc_M']
FX_delta_bc['rslt_bc*_H']=FX_delta_bc['Sb*_H']*FX_delta_bc['Sc*_H']*FX_delta_bc['Gamma_bc_H']
FX_delta_bc['rslt_bc*_L']=FX_delta_bc['Sb*_L']*FX_delta_bc['Sc*_L']*FX_delta_bc['Gamma_bc_L']


# In[422]:


fxd = pd.DataFrame([],columns=['GROUPING','RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=['0'])


# In[423]:


fxd_M_est=sum(FX_delta_agg['Kb_M^2'])+sum(FX_delta_bc['rslt_bc_M'])
fxd_M_1=np.sqrt(sum(FX_delta_agg['Kb_M^2'])+sum(FX_delta_bc['rslt_bc_M']))
fxd_M_2=np.sqrt(sum(FX_delta_agg['Kb_M^2'])+sum(FX_delta_bc['rslt_bc*_M']))


# In[424]:


fxd_H_est=sum(FX_delta_agg['Kb_H^2'])+sum(FX_delta_bc['rslt_bc_H'])
fxd_H_1=np.sqrt(sum(FX_delta_agg['Kb_H^2'])+sum(FX_delta_bc['rslt_bc_H']))
fxd_H_2=np.sqrt(sum(FX_delta_agg['Kb_H^2'])+sum(FX_delta_bc['rslt_bc*_H']))


# In[425]:


fxd_L_est=sum(FX_delta_agg['Kb_L^2'])+sum(FX_delta_bc['rslt_bc_L'])
fxd_L_1=np.sqrt(sum(FX_delta_agg['Kb_L^2'])+sum(FX_delta_bc['rslt_bc_L']))
fxd_L_2=np.sqrt(sum(FX_delta_agg['Kb_L^2'])+sum(FX_delta_bc['rslt_bc*_L']))


# In[426]:


fxd['RISK_FACTOR_CLASS']='FX'
fxd['SENS_TYPE']='DELTA'
fxd['NORMAL']=np.where(fxd_M_est>=0,fxd_M_1,fxd_M_2)
fxd['HIGH']=np.where(fxd_H_est>=0,fxd_H_1,fxd_H_2)
fxd['LOW']=np.where(fxd_L_est>=0,fxd_L_1,fxd_L_2)


# In[427]:


fxd_1=FX_delta[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]


# In[428]:


fxd_3=FX_delta_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                      ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()


# In[429]:


fxd_4=FX_delta_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]


# In[430]:


fxd_decomp=fxd_1.merge(fxd_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left').merge(fxd_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left').merge(fxd,on=['RISK_FACTOR_CLASS'],how='left')


# In[431]:


fxd_decomp=fxd_decomp.drop(['Bucket_b','GROUPING','SENS_TYPE'],axis=1)


# In[432]:


fxd_decomp['M_est']=fxd_M_est
fxd_decomp['H_est']=fxd_H_est
fxd_decomp['L_est']=fxd_L_est


# In[433]:


#case 1
fxd_decomp.loc[(fxd_decomp['M_est']>=0)&(fxd_decomp['Kb_M']>0),'pder_M']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_M'])/fxd_decomp['NORMAL']

fxd_decomp.loc[(fxd_decomp['H_est']>=0)&(fxd_decomp['Kb_H']>0),'pder_H']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_H'])/fxd_decomp['HIGH']

fxd_decomp.loc[(fxd_decomp['L_est']>=0)&(fxd_decomp['Kb_L']>0),'pder_L']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_L'])/fxd_decomp['LOW']


# In[434]:


#case 2
fxd_decomp.loc[(fxd_decomp['M_est']>=0)&(fxd_decomp['Kb_M']==0),'pder_M']=fxd_decomp['gammac_M']/fxd_decomp['NORMAL']

fxd_decomp.loc[(fxd_decomp['H_est']>=0)&(fxd_decomp['Kb_H']==0),'pder_H']=fxd_decomp['gammac_H']/fxd_decomp['HIGH']

fxd_decomp.loc[(fxd_decomp['L_est']>=0)&(fxd_decomp['Kb_L']==0),'pder_L']=fxd_decomp['gammac_L']/fxd_decomp['LOW']


# In[435]:


#case 3
fxd_decomp.loc[(fxd_decomp['M_est']<0)&(fxd_decomp['Kb_M']>0)&(fxd_decomp['Sb*_M']==fxd_decomp['Kb_M']),'pder_M']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1+1/fxd_decomp['Kb_M']*fxd_decomp['gammac_M']))/fxd_decomp['NORMAL']

fxd_decomp.loc[(fxd_decomp['H_est']<0)&(fxd_decomp['Kb_H']>0)&(fxd_decomp['Sb*_H']==fxd_decomp['Kb_H']),'pder_H']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1+1/fxd_decomp['Kb_H']*fxd_decomp['gammac_H']))/fxd_decomp['HIGH']

fxd_decomp.loc[(fxd_decomp['L_est']<0)&(fxd_decomp['Kb_L']>0)&(fxd_decomp['Sb*_L']==fxd_decomp['Kb_L']),'pder_L']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1+1/fxd_decomp['Kb_L']*fxd_decomp['gammac_L']))/fxd_decomp['LOW']


# In[436]:


#case 4
fxd_decomp.loc[(fxd_decomp['M_est']<0)&(fxd_decomp['Kb_M']>0)&(fxd_decomp['Sb*_M']+fxd_decomp['Kb_M']==0),'pder_M']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1-1/fxd_decomp['Kb_M']*fxd_decomp['gammac_M']))/fxd_decomp['NORMAL']

fxd_decomp.loc[(fxd_decomp['H_est']<0)&(fxd_decomp['Kb_H']>0)&(fxd_decomp['Sb*_H']+fxd_decomp['Kb_H']==0),'pder_H']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1-1/fxd_decomp['Kb_H']*fxd_decomp['gammac_H']))/fxd_decomp['HIGH']

fxd_decomp.loc[(fxd_decomp['L_est']<0)&(fxd_decomp['Kb_L']>0)&(fxd_decomp['Sb*_L']+fxd_decomp['Kb_L']==0),'pder_L']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1-1/fxd_decomp['Kb_L']*fxd_decomp['gammac_L']))/fxd_decomp['LOW']


# In[437]:


#case 5
fxd_decomp.loc[(fxd_decomp['M_est']<0)&(fxd_decomp['Kb_M']>0)&(abs(fxd_decomp['Sb*_M'])!=abs(fxd_decomp['Kb_M'])),'pder_M']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_M'])/fxd_decomp['NORMAL']

fxd_decomp.loc[(fxd_decomp['H_est']<0)&(fxd_decomp['Kb_H']>0)&(abs(fxd_decomp['Sb*_H'])!=abs(fxd_decomp['Kb_H'])),'pder_H']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_H'])/fxd_decomp['HIGH']

fxd_decomp.loc[(fxd_decomp['L_est']<0)&(fxd_decomp['Kb_L']>0)&(abs(fxd_decomp['Sb*_L'])!=abs(fxd_decomp['Kb_L'])),'pder_L']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_L'])/fxd_decomp['LOW']


# In[438]:


#case 6
fxd_decomp.loc[(fxd_decomp['M_est']<0)&(fxd_decomp['Kb_M']==0),'pder_M']=0

fxd_decomp.loc[(fxd_decomp['H_est']<0)&(fxd_decomp['Kb_H']==0),'pder_H']=0

fxd_decomp.loc[(fxd_decomp['L_est']<0)&(fxd_decomp['Kb_L']==0),'pder_L']=0


# In[439]:


fxd_decomp=fxd_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]


# In[440]:


fxd_decomp_rslt=FX_RawData[(FX_RawData.SENSITIVITY_TYPE=='Delta')].merge(fxd_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET'],how='left')


# In[441]:


#sum(fxd_decomp_rslt['WEIGHTED_SENSITIVITY']*fxd_decomp_rslt['pder_M'])


# In[442]:


#sum(fxd_decomp_rslt['WEIGHTED_SENSITIVITY']*fxd_decomp_rslt['pder_H'])


# In[443]:


#sum(fxd_decomp_rslt['WEIGHTED_SENSITIVITY']*fxd_decomp_rslt['pder_L'])


# In[444]:


#fxd


# In[ ]:





# In[ ]:





# In[ ]:





# In[445]:


FX_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='FX')]

FX_Position = FX_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                          'RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]

FX_Position = FX_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                   'RISK_FACTOR_BUCKET','SENSITIVITY_TYPE']
                                  ,dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()

FX_vega = FX_Position[(FX_Position['SENSITIVITY_TYPE']=='Vega')]


# In[446]:


FX_vega_kl = FX_vega.rename(
    {'RISK_FACTOR_ID':'RISK_FACTOR_ID_K'
     ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_K'
     ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_K'},axis=1
).merge(FX_vega[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1',
                   'RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
        .rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_L'
                 ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_L'
                 ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_L'},axis=1)
        ,on=['RISK_FACTOR_BUCKET'],how='left')


# In[447]:


FX_vega_kl['Rho_kl_opt_mat_M'] = np.exp(
    -0.01*abs(
        FX_vega_kl['RISK_FACTOR_VERTEX_1_K']-FX_vega_kl['RISK_FACTOR_VERTEX_1_L']
    )/np.minimum(FX_vega_kl['RISK_FACTOR_VERTEX_1_K'],FX_vega_kl['RISK_FACTOR_VERTEX_1_L']))


# In[448]:


FX_vega_kl['Rho_kl_M']=np.minimum(FX_vega_kl['Rho_kl_opt_mat_M'],1)
FX_vega_kl['rslt_kl_M']=FX_vega_kl['Rho_kl_M']*FX_vega_kl['WEIGHTED_SENSITIVITY_K']*FX_vega_kl['WEIGHTED_SENSITIVITY_L']
FX_vega_kl['Rho_kl_H']=np.minimum(1,High_Multipler*FX_vega_kl['Rho_kl_M'])
FX_vega_kl['rslt_kl_H']=FX_vega_kl['Rho_kl_H']*FX_vega_kl['WEIGHTED_SENSITIVITY_K']*FX_vega_kl['WEIGHTED_SENSITIVITY_L']
FX_vega_kl['Rho_kl_L']=np.maximum(Low_Multipler1*FX_vega_kl['Rho_kl_M']-1,Low_Multipler2*FX_vega_kl['Rho_kl_M'])
FX_vega_kl['rslt_kl_L']=FX_vega_kl['Rho_kl_L']*FX_vega_kl['WEIGHTED_SENSITIVITY_K']*FX_vega_kl['WEIGHTED_SENSITIVITY_L']


# In[449]:


FX_vega_kl.loc[(FX_vega_kl.RISK_FACTOR_ID_K==FX_vega_kl.RISK_FACTOR_ID_L)
                 &(FX_vega_kl.RISK_FACTOR_VERTEX_1_K==FX_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=0
FX_vega_kl.loc[(FX_vega_kl.RISK_FACTOR_ID_K==FX_vega_kl.RISK_FACTOR_ID_L)
                 &(FX_vega_kl.RISK_FACTOR_VERTEX_1_K==FX_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=0
FX_vega_kl.loc[(FX_vega_kl.RISK_FACTOR_ID_K==FX_vega_kl.RISK_FACTOR_ID_L)
                 &(FX_vega_kl.RISK_FACTOR_VERTEX_1_K==FX_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=0

FX_vega_kl.loc[(FX_vega_kl.RISK_FACTOR_ID_K!=FX_vega_kl.RISK_FACTOR_ID_L)
                 |(FX_vega_kl.RISK_FACTOR_VERTEX_1_K!=FX_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=FX_vega_kl['WEIGHTED_SENSITIVITY_L']*FX_vega_kl['Rho_kl_M']
FX_vega_kl.loc[(FX_vega_kl.RISK_FACTOR_ID_K!=FX_vega_kl.RISK_FACTOR_ID_L)
                 |(FX_vega_kl.RISK_FACTOR_VERTEX_1_K!=FX_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=FX_vega_kl['WEIGHTED_SENSITIVITY_L']*FX_vega_kl['Rho_kl_H']
FX_vega_kl.loc[(FX_vega_kl.RISK_FACTOR_ID_K!=FX_vega_kl.RISK_FACTOR_ID_L)
                 |(FX_vega_kl.RISK_FACTOR_VERTEX_1_K!=FX_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=FX_vega_kl['WEIGHTED_SENSITIVITY_L']*FX_vega_kl['Rho_kl_L']


# In[450]:


FX_vega_agg=FX_vega.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()


# In[451]:


FX_vega_bc=FX_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                                   ).merge(FX_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c'
                                                                  ,'WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                                           ,on =['RISK_FACTOR_CLASS'],how='left')


# In[452]:


FX_vega_bc = FX_vega_bc.loc[(FX_vega_bc.Bucket_b!=FX_vega_bc.Bucket_c),:].reset_index(drop=True)


# In[453]:


FX_vega_bc['Gamma_bc_M']=FX_Gamma
FX_vega_bc['Gamma_bc_H']=np.minimum(1,High_Multipler*FX_vega_bc['Gamma_bc_M'])
FX_vega_bc['Gamma_bc_L']=np.maximum(Low_Multipler1*FX_vega_bc['Gamma_bc_M']-1,Low_Multipler2*FX_vega_bc['Gamma_bc_M'])


# In[454]:


FX_vega_bc['rslt_bc_M']=FX_vega_bc['WS_b']*FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_M']
FX_vega_bc['rslt_bc_H']=FX_vega_bc['WS_b']*FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_H']
FX_vega_bc['rslt_bc_L']=FX_vega_bc['WS_b']*FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_L']


# In[455]:


FX_vega_bc['gammac_M']=FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_M']
FX_vega_bc['gammac_H']=FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_H']
FX_vega_bc['gammac_L']=FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_L']


# In[456]:


FX_vega_agg=FX_vega_agg.merge(
    FX_vega_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']],on='RISK_FACTOR_BUCKET',how='left'
).groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']
                                      ,dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()
FX_vega_agg['Sb_H']=FX_vega_agg['WEIGHTED_SENSITIVITY']
FX_vega_agg['Sb_L']=FX_vega_agg['WEIGHTED_SENSITIVITY']
FX_vega_agg['Kb_M']=np.sqrt(FX_vega_agg['rslt_kl_M'])
FX_vega_agg['Kb_H']=np.sqrt(FX_vega_agg['rslt_kl_H'])
FX_vega_agg['Kb_L']=np.sqrt(FX_vega_agg['rslt_kl_L'])
FX_vega_agg = FX_vega_agg.rename({'WEIGHTED_SENSITIVITY':'Sb_M','rslt_kl_M':'Kb_M^2','rslt_kl_H':'Kb_H^2','rslt_kl_L':'Kb_L^2'},axis=1)


# In[457]:


FX_vega_agg['Sb*_M']=np.maximum(np.minimum(FX_vega_agg['Kb_M'],FX_vega_agg['Sb_M']),-FX_vega_agg['Kb_M'])
FX_vega_agg['Sb*_H']=np.maximum(np.minimum(FX_vega_agg['Kb_H'],FX_vega_agg['Sb_H']),-FX_vega_agg['Kb_H'])
FX_vega_agg['Sb*_L']=np.maximum(np.minimum(FX_vega_agg['Kb_L'],FX_vega_agg['Sb_L']),-FX_vega_agg['Kb_L'])

FX_vega_bc=FX_vega_bc.merge(
    FX_vega_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
    ,left_on='Bucket_b',right_on='RISK_FACTOR_BUCKET',how='left')

FX_vega_bc=FX_vega_bc.merge(
    FX_vega_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
    ,left_on='Bucket_c',right_on='RISK_FACTOR_BUCKET',how='left')

FX_vega_bc=FX_vega_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)

FX_vega_bc.loc[FX_vega_bc['Gamma_bc_M']==1,'rslt_bc*_M']=0
FX_vega_bc.loc[FX_vega_bc['Gamma_bc_M']!=1,'rslt_bc*_M']=FX_vega_bc['Sb*_M']*FX_vega_bc['Sc*_M']*FX_vega_bc['Gamma_bc_M']
FX_vega_bc.loc[FX_vega_bc['Gamma_bc_H']==1,'rslt_bc*_H']=0
FX_vega_bc.loc[FX_vega_bc['Gamma_bc_H']!=1,'rslt_bc*_H']=FX_vega_bc['Sb*_H']*FX_vega_bc['Sc*_H']*FX_vega_bc['Gamma_bc_H']
FX_vega_bc.loc[FX_vega_bc['Gamma_bc_L']==1,'rslt_bc*_L']=0
FX_vega_bc.loc[FX_vega_bc['Gamma_bc_L']!=1,'rslt_bc*_L']=FX_vega_bc['Sb*_L']*FX_vega_bc['Sc*_L']*FX_vega_bc['Gamma_bc_L']


# In[458]:


fxv=pd.DataFrame([],columns=['GROUPING','RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])


# In[459]:


fxv_M_est=sum(FX_vega_agg['Kb_M^2'])+sum(FX_vega_bc['rslt_bc_M'])
fxv_M_1=np.sqrt(sum(FX_vega_agg['Kb_M^2'])+sum(FX_vega_bc['rslt_bc_M']))
fxv_M_2=np.sqrt(sum(FX_vega_agg['Kb_M^2'])+sum(FX_vega_bc['rslt_bc*_M']))


# In[460]:


fxv_H_est=sum(FX_vega_agg['Kb_H^2'])+sum(FX_vega_bc['rslt_bc_H'])
fxv_H_1=np.sqrt(sum(FX_vega_agg['Kb_H^2'])+sum(FX_vega_bc['rslt_bc_H']))
fxv_H_2=np.sqrt(sum(FX_vega_agg['Kb_H^2'])+sum(FX_vega_bc['rslt_bc*_H']))


# In[461]:


fxv_L_est=sum(FX_vega_agg['Kb_L^2'])+sum(FX_vega_bc['rslt_bc_L'])
fxv_L_1=np.sqrt(sum(FX_vega_agg['Kb_L^2'])+sum(FX_vega_bc['rslt_bc_L']))
fxv_L_2=np.sqrt(sum(FX_vega_agg['Kb_L^2'])+sum(FX_vega_bc['rslt_bc*_L']))


# In[462]:


fxv['RISK_FACTOR_CLASS']='FX'
fxv['SENS_TYPE']='VEGA'
fxv['NORMAL']=np.where(fxv_M_est>=0,fxv_M_1,fxv_M_2)
fxv['HIGH']=np.where(fxv_H_est>=0,fxv_H_1,fxv_H_2)
fxv['LOW']=np.where(fxv_L_est>=0,fxv_L_1,fxv_L_2)


# In[463]:


fxv_1=FX_vega[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]


# In[464]:


fxv_2=FX_vega_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                             ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()


# In[465]:


fxv_3=FX_vega_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                             ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()


# In[466]:


fxv_4=FX_vega_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]


# In[467]:


fxv_decomp=fxv_1.merge(fxv_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET']
                           ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                           ,how='left')\
.merge(fxv_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
.merge(fxv_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
.merge(fxv,on=['RISK_FACTOR_CLASS'],how='left')


# In[468]:


fxv_decomp=fxv_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','Bucket_b','GROUPING','SENS_TYPE'],axis=1)


# In[469]:


fxv_decomp['M_est']=fxv_M_est
fxv_decomp['H_est']=fxv_H_est
fxv_decomp['L_est']=fxv_L_est


# In[470]:


#case 1
fxv_decomp.loc[(fxv_decomp['M_est']>=0)&(fxv_decomp['Kb_M']>0),'pder_M']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_M']+fxv_decomp['gammac_M'])/fxv_decomp['NORMAL']

fxv_decomp.loc[(fxv_decomp['H_est']>=0)&(fxv_decomp['Kb_H']>0),'pder_H']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_H']+fxv_decomp['gammac_H'])/fxv_decomp['HIGH']

fxv_decomp.loc[(fxv_decomp['L_est']>=0)&(fxv_decomp['Kb_L']>0),'pder_L']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_L']+fxv_decomp['gammac_L'])/fxv_decomp['LOW']


# In[471]:


#case 2
fxv_decomp.loc[(fxv_decomp['M_est']>=0)&(fxv_decomp['Kb_M']==0),'pder_M']=fxv_decomp['gammac_M']/fxv_decomp['NORMAL']

fxv_decomp.loc[(fxv_decomp['H_est']>=0)&(fxv_decomp['Kb_H']==0),'pder_H']=fxv_decomp['gammac_H']/fxv_decomp['HIGH']

fxv_decomp.loc[(fxv_decomp['L_est']>=0)&(fxv_decomp['Kb_L']==0),'pder_L']=fxv_decomp['gammac_L']/fxv_decomp['LOW']


# In[472]:


#case 3
fxv_decomp.loc[(fxv_decomp['M_est']<0)&(fxv_decomp['Kb_M']>0)&(fxv_decomp['Sb*_M']==fxv_decomp['Kb_M']),'pder_M']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_M'])*(1+1/fxv_decomp['Kb_M']*fxv_decomp['gammac_M']))/fxv_decomp['NORMAL']

fxv_decomp.loc[(fxv_decomp['H_est']<0)&(fxv_decomp['Kb_H']>0)&(fxv_decomp['Sb*_H']==fxv_decomp['Kb_H']),'pder_H']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_H'])*(1+1/fxv_decomp['Kb_H']*fxv_decomp['gammac_H']))/fxv_decomp['HIGH']

fxv_decomp.loc[(fxv_decomp['L_est']<0)&(fxv_decomp['Kb_L']>0)&(fxv_decomp['Sb*_L']==fxv_decomp['Kb_L']),'pder_L']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_L'])*(1+1/fxv_decomp['Kb_L']*fxv_decomp['gammac_L']))/fxv_decomp['LOW']


# In[473]:


#case 4
fxv_decomp.loc[(fxv_decomp['M_est']<0)&(fxv_decomp['Kb_M']>0)&(fxv_decomp['Sb*_M']+fxv_decomp['Kb_M']==0),'pder_M']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_M'])*(1-1/fxv_decomp['Kb_M']*fxv_decomp['gammac_M']))/fxv_decomp['NORMAL']

fxv_decomp.loc[(fxv_decomp['H_est']<0)&(fxv_decomp['Kb_H']>0)&(fxv_decomp['Sb*_H']+fxv_decomp['Kb_H']==0),'pder_H']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_H'])*(1-1/fxv_decomp['Kb_H']*fxv_decomp['gammac_H']))/fxv_decomp['HIGH']

fxv_decomp.loc[(fxv_decomp['L_est']<0)&(fxv_decomp['Kb_L']>0)&(fxv_decomp['Sb*_L']+fxv_decomp['Kb_L']==0),'pder_L']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_L'])*(1-1/fxv_decomp['Kb_L']*fxv_decomp['gammac_L']))/fxv_decomp['LOW']


# In[474]:


#case 5
fxv_decomp.loc[(fxv_decomp['M_est']<0)&(fxv_decomp['Kb_M']>0)&(abs(fxv_decomp['Sb*_M'])!=abs(fxv_decomp['Kb_M'])),'pder_M']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_M']+fxv_decomp['gammac_M'])/fxv_decomp['NORMAL']

fxv_decomp.loc[(fxv_decomp['H_est']<0)&(fxv_decomp['Kb_H']>0)&(abs(fxv_decomp['Sb*_H'])!=abs(fxv_decomp['Kb_H'])),'pder_H']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_H']+fxv_decomp['gammac_H'])/fxv_decomp['HIGH']

fxv_decomp.loc[(fxv_decomp['L_est']<0)&(fxv_decomp['Kb_L']>0)&(abs(fxv_decomp['Sb*_L'])!=abs(fxv_decomp['Kb_L'])),'pder_L']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_L']+fxv_decomp['gammac_L'])/fxv_decomp['LOW']


# In[475]:


#case 6
fxv_decomp.loc[(fxv_decomp['M_est']<0)&(fxv_decomp['Kb_M']==0),'pder_M']=0

fxv_decomp.loc[(fxv_decomp['H_est']<0)&(fxv_decomp['Kb_H']==0),'pder_H']=0

fxv_decomp.loc[(fxv_decomp['L_est']<0)&(fxv_decomp['Kb_L']==0),'pder_L']=0


# In[476]:


fxv_decomp=fxv_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]


# In[477]:


fxv_decomp_rslt=FX_RawData[(FX_RawData.SENSITIVITY_TYPE=='Vega')].merge(fxv_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'],how='left')


# In[478]:


fxv_decomp_rslt=fxv_decomp_rslt.fillna({'pder_M':0,'pder_H':0,'pder_L':0})


# In[479]:


#sum(fxv_decomp_rslt['WEIGHTED_SENSITIVITY']*fxv_decomp_rslt['pder_M'])


# In[480]:


#sum(fxv_decomp_rslt['WEIGHTED_SENSITIVITY']*fxv_decomp_rslt['pder_H'])


# In[481]:


#sum(fxv_decomp_rslt['WEIGHTED_SENSITIVITY']*fxv_decomp_rslt['pder_L'])


# In[482]:


#fxv


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[483]:


FX_curvature = FX_Position.query('SENSITIVITY_TYPE=="Curvature Up"|SENSITIVITY_TYPE=="Curvature Down"')


# In[484]:


FX_curvature = FX_curvature.assign(max_0_square=np.square(np.maximum(FX_curvature['WEIGHTED_SENSITIVITY'],0)))


# In[485]:


FX_curvature_agg = FX_curvature.groupby(
    ['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],dropna=False
).agg({'WEIGHTED_SENSITIVITY':'sum','max_0_square':'sum'}).reset_index()


# In[486]:


FX_curvature_agg['max_0_k']=np.sqrt(FX_curvature_agg['max_0_square'])


# In[487]:


FX_curvature_agg=FX_curvature_agg.pivot(index=('RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET')
                         ,columns='SENSITIVITY_TYPE')


# In[488]:


FX_curvature_agg.columns=['/'.join(i) for i in FX_curvature_agg.columns]
FX_curvature_agg=FX_curvature_agg.reset_index()


# In[489]:


FX_curvature_agg['Kb+_M']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Up'])))
FX_curvature_agg['Kb-_M']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Down'])))
FX_curvature_agg['Kb_M']=np.maximum(FX_curvature_agg['Kb+_M'],FX_curvature_agg['Kb-_M'])
FX_curvature_agg['Kb_M^2']=np.square(FX_curvature_agg['Kb_M'])
FX_curvature_agg['Sb_M']=np.select([(FX_curvature_agg['Kb_M'] == FX_curvature_agg['Kb+_M']),
                                      (FX_curvature_agg['Kb_M'] != FX_curvature_agg['Kb+_M'])],
                                     [(FX_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Up']),
                                      (FX_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Down'])])


# In[490]:


FX_curvature_agg['Kb+_H']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Up'])))
FX_curvature_agg['Kb-_H']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Down'])))
FX_curvature_agg['Kb_H']=np.maximum(FX_curvature_agg['Kb+_H'],FX_curvature_agg['Kb-_H'])
FX_curvature_agg['Kb_H^2']=np.square(FX_curvature_agg['Kb_H'])
FX_curvature_agg['Sb_H']=np.select([(FX_curvature_agg['Kb_H'] == FX_curvature_agg['Kb+_H']),
                                      (FX_curvature_agg['Kb_H'] != FX_curvature_agg['Kb+_H'])],
                                     [(FX_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Up']),
                                      (FX_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Down'])])


# In[491]:


FX_curvature_agg['Kb+_L']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Up'])))
FX_curvature_agg['Kb-_L']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Down'])))
FX_curvature_agg['Kb_L']=np.maximum(FX_curvature_agg['Kb+_L'],FX_curvature_agg['Kb-_L'])
FX_curvature_agg['Kb_L^2']=np.square(FX_curvature_agg['Kb_L'])
FX_curvature_agg['Sb_L']=np.select([(FX_curvature_agg['Kb_L'] == FX_curvature_agg['Kb+_L']),
                                      (FX_curvature_agg['Kb_L'] != FX_curvature_agg['Kb+_L'])],
                                     [(FX_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Up']),
                                      (FX_curvature_agg['WEIGHTED_SENSITIVITY/Curvature Down'])])


# In[492]:


FX_curvature_agg['max']=np.select([(FX_curvature_agg['Kb_M'] == FX_curvature_agg['Kb+_M']),
                                     (FX_curvature_agg['Kb_M'] != FX_curvature_agg['Kb+_M'])],
                                    [(FX_curvature_agg['max_0_k/Curvature Up']),
                                     (FX_curvature_agg['max_0_k/Curvature Down'])])


# In[493]:


FX_curvature_agg['sign']=np.select([(FX_curvature_agg['Kb_M'] == FX_curvature_agg['Kb+_M']),
                                      (FX_curvature_agg['Kb_M'] != FX_curvature_agg['Kb+_M'])],
                                     ['Curvature Up','Curvature Down'])


# In[494]:


FX_curvature_bc=FX_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Sb_M']]
FX_curvature_bc=FX_curvature_bc.rename(
    {'Sb_M':'Sb','RISK_FACTOR_BUCKET':'Bucket_b'},axis=1
).merge(FX_curvature_bc.rename(
    {'Sb_M':'Sc','RISK_FACTOR_BUCKET':'Bucket_c'},axis=1
),on=['RISK_FACTOR_CLASS'],how='left')
FX_curvature_bc=FX_curvature_bc[(FX_curvature_bc['Bucket_b']!=FX_curvature_bc['Bucket_c'])]


# In[495]:


FX_curvature_bc.loc[(FX_curvature_bc['Sb']<0) & (FX_curvature_bc['Sc']<0),'Psi']=0
FX_curvature_bc.loc[(FX_curvature_bc['Sb']>=0) | (FX_curvature_bc['Sc']>=0),'Psi']=1
FX_curvature_bc['Gamma_bc']=FX_Gamma
FX_curvature_bc['Gamma_bc_M']=np.square(FX_curvature_bc['Gamma_bc'])


# In[496]:


FX_curvature_bc['rslt_bc_M']=FX_curvature_bc['Gamma_bc_M']*FX_curvature_bc['Psi']*FX_curvature_bc['Sb']*FX_curvature_bc['Sc']
FX_curvature_bc['Gamma_bc_H']=np.square(np.minimum(1,FX_curvature_bc['Gamma_bc']*High_Multipler))
FX_curvature_bc['Gamma_bc_L']=np.square(np.maximum((Low_Multipler1*FX_curvature_bc['Gamma_bc']-1),(Low_Multipler2*FX_curvature_bc['Gamma_bc'])))
FX_curvature_bc['rslt_bc_H']=FX_curvature_bc['Gamma_bc_H']*FX_curvature_bc['Psi']*FX_curvature_bc['Sb']*FX_curvature_bc['Sc']
FX_curvature_bc['rslt_bc_L']=FX_curvature_bc['Gamma_bc_L']*FX_curvature_bc['Psi']*FX_curvature_bc['Sb']*FX_curvature_bc['Sc']


# In[497]:


FX_curvature_bc['gammac_M']=FX_curvature_bc['Gamma_bc_M']*FX_curvature_bc['Psi']*FX_curvature_bc['Sc']
FX_curvature_bc['gammac_H']=FX_curvature_bc['Gamma_bc_H']*FX_curvature_bc['Psi']*FX_curvature_bc['Sc']
FX_curvature_bc['gammac_L']=FX_curvature_bc['Gamma_bc_L']*FX_curvature_bc['Psi']*FX_curvature_bc['Sc']


# In[498]:


fxc_M_est=sum(FX_curvature_agg['Kb_M^2'])+sum(FX_curvature_bc['rslt_bc_M'])
fxc_H_est=sum(FX_curvature_agg['Kb_H^2'])+sum(FX_curvature_bc['rslt_bc_H'])
fxc_L_est=sum(FX_curvature_agg['Kb_L^2'])+sum(FX_curvature_bc['rslt_bc_L'])


# In[499]:


fxc_M = np.sqrt(np.maximum(0,sum(FX_curvature_agg['Kb_M^2'])+sum(FX_curvature_bc['rslt_bc_M'])))
fxc_H = np.sqrt(np.maximum(0,sum(FX_curvature_agg['Kb_H^2'])+sum(FX_curvature_bc['rslt_bc_H'])))
fxc_L = np.sqrt(np.maximum(0,sum(FX_curvature_agg['Kb_L^2'])+sum(FX_curvature_bc['rslt_bc_L'])))


# In[500]:


fxc=pd.DataFrame([],columns=['GROUPING','RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])


# In[501]:


fxc['RISK_FACTOR_CLASS']='FX'
fxc['SENS_TYPE']='CURVATURE'
fxc['NORMAL']=fxc_M
fxc['HIGH']=fxc_H
fxc['LOW']=fxc_L


# In[502]:


fxc_1=FX_curvature[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]


# In[503]:


fxc_3=FX_curvature_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                                  ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()


# In[504]:


fxc_4=FX_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','max','sign']]


# In[505]:


fxc_decomp=fxc_1.merge(fxc_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET']
                           ,right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
.merge(fxc_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
.merge(fxc,on=['RISK_FACTOR_CLASS'],how='left')


# In[506]:


fxc_decomp=fxc_decomp.drop(['Bucket_b','GROUPING','SENS_TYPE'],axis=1)


# In[507]:


fxc_decomp['M_est']=fxc_M_est
fxc_decomp['H_est']=fxc_H_est
fxc_decomp['L_est']=fxc_L_est


# In[508]:


fxc_decomp=fxc_decomp[(fxc_decomp.SENSITIVITY_TYPE==fxc_decomp.sign)]


# In[509]:


#case 1/2
fxc_decomp.loc[(fxc_decomp['M_est']>=0),'pder_M']=(fxc_decomp['max']+fxc_decomp['gammac_M'])/fxc_decomp['NORMAL']

fxc_decomp.loc[(fxc_decomp['H_est']>=0),'pder_H']=(fxc_decomp['max']+fxc_decomp['gammac_H'])/fxc_decomp['HIGH']

fxc_decomp.loc[(fxc_decomp['L_est']>=0),'pder_L']=(fxc_decomp['max']+fxc_decomp['gammac_L'])/fxc_decomp['LOW']


# In[510]:


#case 3 
fxc_decomp.loc[(fxc_decomp['M_est']<0),'pder_M']=0
fxc_decomp.loc[(fxc_decomp['H_est']<0),'pder_H']=0
fxc_decomp.loc[(fxc_decomp['L_est']<0),'pder_L']=0


# In[511]:


fxc_decomp=fxc_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','pder_M','pder_H','pder_L']]


# In[512]:


fxc_decomp_rslt=FX_RawData.query('SENSITIVITY_TYPE=="Curvature Up"|SENSITIVITY_TYPE=="Curvature Down"').merge(fxc_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],how='right')


# In[513]:


#sum(fxc_decomp_rslt['WEIGHTED_SENSITIVITY']*fxc_decomp_rslt['pder_M'])


# In[514]:


#sum(fxc_decomp_rslt['WEIGHTED_SENSITIVITY']*fxc_decomp_rslt['pder_H'])


# In[515]:


#sum(fxc_decomp_rslt['WEIGHTED_SENSITIVITY']*fxc_decomp_rslt['pder_L'])


# In[516]:


#fxc


# In[ ]:





# In[ ]:





# In[524]:


pos=pd.concat([GIRR_delta,GIRR_vega,GIRR_curvature,CSR_delta,CSRNC_delta,CMTY_delta,CMTY_vega,CMTY_curvature,FX_delta,FX_vega,FX_curvature],join="outer",ignore_index=True)


# In[526]:


pos.loc[:,'PORTFOLIO_LEVEL']='L1'


# In[527]:


pos=pos[['PORTFOLIO_LEVEL','RISK_FACTOR_ID', 'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2'
         , 'RISK_FACTOR_CLASS', 'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE'
         , 'SEC_ISSUER', 'SEC_TRANCHE', 'COMM_ASSET', 'COMM_LOCATION'
         , 'SENSITIVITY_TYPE', 'WEIGHTED_SENSITIVITY']]


# In[529]:


bucket=pd.concat([GIRR_delta_agg,GIRR_vega_agg,GIRR_curvature_agg,CSR_delta_agg,CSRNC_delta_agg,CMTY_delta_agg,CMTY_vega_agg,CMTY_curvature_agg,FX_delta_agg,FX_vega_agg,FX_curvature_agg],join="outer",ignore_index=True)


# In[534]:


bucket.loc[:,'PORTFOLIO_LEVEL']='L1'


# In[535]:


bucket=bucket[['PORTFOLIO_LEVEL','RISK_FACTOR_CLASS', 'RISK_FACTOR_BUCKET'
               , 'Kb+_M', 'Kb-_M', 'Kb_M', 'Kb_M^2', 'Sb_M'
               , 'Kb+_H', 'Kb-_H', 'Kb_H', 'Kb_H^2', 'Sb_H'
               , 'Kb+_L', 'Kb-_L', 'Kb_L', 'Kb_L^2', 'Sb_L'
               , 'Sb*_M', 'Sb*_H', 'Sb*_L']]


# In[760]:


class_=pd.concat([girrd, girrv, girrc, csrd, csrncd, cmtyd, cmtyv, cmtyc, fxd, fxv, fxc],ignore_index=True)


# In[761]:


class_=class_.pivot(index=('GROUPING','RISK_FACTOR_CLASS')
                     ,columns='SENS_TYPE')


# In[762]:


class_.columns=['_'.join(i) for i in class_.columns]


# In[763]:


class_=class_.reset_index()


# In[764]:


class_.loc[:,'NORMAL'] = class_.loc[:,['NORMAL_DELTA','NORMAL_VEGA','NORMAL_CURVATURE']].sum(axis=1)
class_.loc[:,'HIGH'] = class_.loc[:,['HIGH_DELTA','HIGH_VEGA','HIGH_CURVATURE']].sum(axis=1)
class_.loc[:,'LOW'] = class_.loc[:,['LOW_DELTA','LOW_VEGA','LOW_CURVATURE']].sum(axis=1)


# In[765]:


class_.loc[:,'RISK_CHARGE']=class_.loc[:,['NORMAL','HIGH','LOW']].max(axis=1)


# In[766]:


class_['MAX_SIGN']=class_[['NORMAL', 'HIGH', 'LOW']].idxmax(1)


# In[767]:


class_.loc[:,'GROUP_TYPE']='PORTFOLIO_LEVEL'
class_.loc[:,'GROUP_VALUE']='L1'


# In[ ]:





# In[680]:


decomp_rslt=pd.concat([girrd_decomp_rslt,girrv_decomp_rslt,girrc_decomp_rslt
                       ,csrd_decomp_rslt,csrncd_decomp_rslt
                       ,cmtyd_decomp_rslt,cmtyv_decomp_rslt,cmtyc_decomp_rslt
                       ,fxd_decomp_rslt,fxv_decomp_rslt,fxc_decomp_rslt],ignore_index=True)


# In[671]:


#decomp_rslt=decomp_rslt.groupby(['PORTFOLIO_LEVEL','INSTRUMENT_ID','RISK_FACTOR_ID',
#       'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2', 'RISK_FACTOR_CLASS',
#       'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE', 'SEC_ISSUER', 'SEC_TRANCHE',
#       'COMM_ASSET', 'COMM_LOCATION', 'SENSITIVITY_TYPE',
#       'SENSITIVITY_VAL_INSTRUMENT_CURR', 'INSTRUMENT_CURRENCY',
#        'MARK_TO_MARKET', 'DATA_DATE', 'pder_M',
#       'pder_H', 'pder_L'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()


# In[682]:


decomp_rslt=decomp_rslt.merge(class_[['RISK_FACTOR_CLASS','MAX_SIGN']],on=['RISK_FACTOR_CLASS'],how='left')


# In[683]:


decomp_rslt.loc[decomp_rslt.MAX_SIGN=='NORMAL','PARTIAL_DERIVATIVE']=decomp_rslt.pder_M
decomp_rslt.loc[decomp_rslt.MAX_SIGN=='HIGH','PARTIAL_DERIVATIVE']=decomp_rslt.pder_H
decomp_rslt.loc[decomp_rslt.MAX_SIGN=='LOW','PARTIAL_DERIVATIVE']=decomp_rslt.pder_L


# In[684]:


decomp_rslt['CONTRIBUTION']=decomp_rslt.PARTIAL_DERIVATIVE*decomp_rslt.WEIGHTED_SENSITIVITY


# In[685]:


decomp_rslt=decomp_rslt[['PORTFOLIO_LEVEL','INSTRUMENT_ID','RISK_FACTOR_ID',
       'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2', 'RISK_FACTOR_CLASS',
       'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE', 'SEC_ISSUER', 'SEC_TRANCHE',
       'COMM_ASSET', 'COMM_LOCATION', 'SENSITIVITY_TYPE', 
       'WEIGHTED_SENSITIVITY', 'CONTRIBUTION']]


# In[686]:


decomp_rslt.loc[decomp_rslt.SENSITIVITY_TYPE=='Curvature Up','SENSITIVITY_TYPE']='Curvature'
decomp_rslt.loc[decomp_rslt.SENSITIVITY_TYPE=='Curvature Down','SENSITIVITY_TYPE']='Curvature'


# In[ ]:





# In[687]:


riskfactor=decomp_rslt.groupby(['RISK_FACTOR_ID',
       'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2', 'RISK_FACTOR_CLASS',
       'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE', 'SEC_ISSUER', 'SEC_TRANCHE',
       'COMM_ASSET', 'COMM_LOCATION', 'SENSITIVITY_TYPE'],dropna=False
                              ).agg({'WEIGHTED_SENSITIVITY':'sum', 'CONTRIBUTION':'sum'}).reset_index()


# In[690]:


riskfactor.loc[:,'PORTFOLIO_LEVEL']='L1'


# In[692]:


riskfactor=riskfactor[['PORTFOLIO_LEVEL', 'RISK_FACTOR_ID', 'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2',
       'RISK_FACTOR_CLASS', 'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE',
       'SEC_ISSUER', 'SEC_TRANCHE', 'COMM_ASSET', 'COMM_LOCATION',
       'SENSITIVITY_TYPE', 'WEIGHTED_SENSITIVITY', 'CONTRIBUTION']]


# In[ ]:





# In[694]:


level3=decomp_rslt.groupby(['PORTFOLIO_LEVEL','RISK_FACTOR_CLASS','SENSITIVITY_TYPE'
                            , 'RISK_FACTOR_ID', 'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2'
                            , 'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE'],dropna=False
                          ).agg({'WEIGHTED_SENSITIVITY':'sum','CONTRIBUTION':'sum'}).reset_index()


# In[701]:


level3=level3.rename({'PORTFOLIO_LEVEL':'GROUP_VALUE'},axis=1)
level3.loc[:,'GROUP_TYPE']='L3'


# In[704]:


level3=level3[['GROUP_TYPE', 'GROUP_VALUE', 'RISK_FACTOR_CLASS', 'SENSITIVITY_TYPE',
       'RISK_FACTOR_ID', 'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2',
       'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE', 'WEIGHTED_SENSITIVITY',
       'CONTRIBUTION']]


# In[768]:


class_=class_[['GROUP_TYPE','GROUP_VALUE','RISK_FACTOR_CLASS'
               , 'HIGH_DELTA', 'NORMAL_DELTA', 'LOW_DELTA'
               , 'HIGH_VEGA', 'NORMAL_VEGA', 'LOW_VEGA'
               , 'HIGH_CURVATURE', 'NORMAL_CURVATURE', 'LOW_CURVATURE'
               , 'RISK_CHARGE']]
class_.columns=[['GROUP_TYPE','GROUP_VALUE','RISK_FACTOR_CLASS'
               , 'DELTA_HIGH', 'DELTA_NORMAL', 'DELTA_LOW'
               , 'VEGA_HIGH', 'VEGA_NORMAL', 'VEGA_LOW'
               , 'CURVATURE_HIGH', 'CURVATURE_NORMAL', 'CURVATURE_LOW'
               , 'SBA_RISK_CHARGE']]


# In[780]:


class_.columns=pd.DataFrame(list(class_.columns)).loc[:,0]


# In[1682]:


#pos.to_csv('pos_1203.csv',index=False)
#bucket.to_csv('bucket_1203.csv',index=False)
#class_.to_csv('class_1203.csv',index=False)
#decomp_rslt.to_csv('decomp_rslt_1203.csv',index=False)
#level3.to_csv('level3_1203.csv',index=False)


# In[729]:


pos=pos.rename({'WEIGHTED_SENSITIVITY':'SENSITIVITY_VAL_REPORTING_CURR_CNY'},axis=1)


# In[741]:


bucket.columns=['PORTFOLIO_LEVEL','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'
                ,'KB_P_M','KB_M_M','KB_M','KB2_M','SB_M'
                ,'KB_P_H','KB_M_H','KB_H','KB2_H','SB_H'
                ,'KB_P_L','KB_M_L','KB_L','KB2_L','SB_L'
                ,'SB_STAR_M','SB_STAR_H','SB_STAR_L']


# In[790]:


pos=pos.astype(object).where(pd.notnull(pos), None)
bucket=bucket.astype(object).where(pd.notnull(bucket), None)
class_=class_.astype(object).where(pd.notnull(class_), None)
riskfactor=riskfactor.astype(object).where(pd.notnull(riskfactor), None)
level3=level3.astype(object).where(pd.notnull(level3), None)


# In[785]:


db=pymysql.connect(host='82.156.70.141',
                   port=3306,
                   db='ry-vue',
                   user='mrdbuser',
                   password='Findeck^2022',
                   charset='utf8')
# create cursor
cursor=db.cursor()

########## result: risk factor##########
cursor.execute("truncate table `TB_26_FRTB_SENSITIVITY_RESULT_RISKFACTOR`;")
db.commit()

cols1 = "`,`".join(pos.columns)

# Insert DataFrame recrds one by one.
for i,row in pos.iterrows():
    sql1 = "INSERT INTO `TB_26_FRTB_SENSITIVITY_RESULT_RISKFACTOR` (`" +cols1 + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql1, tuple(row))
db.commit()

########## result: bucket ##########
cursor.execute("truncate table `TB_26_FRTB_SENSITIVITY_RESULT_BUCKET`;")
db.commit()

cols2 = "`,`".join(bucket.columns)

# Insert DataFrame recrds one by one.
for i,row in bucket.iterrows():
    sql2 = "INSERT INTO `TB_26_FRTB_SENSITIVITY_RESULT_BUCKET` (`" +cols2 + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql2, tuple(row))
db.commit()

########## result: class ##########
cursor.execute("truncate table `TB_26_FRTB_SENSITIVITY_RESULT_RISKCLASS`;")
db.commit()

cols3 = "`,`".join(class_.columns)

# Insert DataFrame recrds one by one.
for i,row in class_.iterrows():
    sql3 = "INSERT INTO `TB_26_FRTB_SENSITIVITY_RESULT_RISKCLASS` (`" +cols3 + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql3, tuple(row))
db.commit()

########## decomp: risk factor##########
cursor.execute("truncate table `TB_26_FRTB_SENSITIVITY_DECOMP_RISKFACTOR`;")
db.commit()

cols4 = "`,`".join(riskfactor.columns)

# Insert DataFrame recrds one by one.
for i,row in riskfactor.iterrows():
    sql4 = "INSERT INTO `TB_26_FRTB_SENSITIVITY_DECOMP_RISKFACTOR` (`" +cols4 + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql4, tuple(row))
db.commit()

########## decomp: portfolio ##########
cursor.execute("truncate table `TB_26_FRTB_SENSITIVITY_DECOMP_PORTFOLIO`;")
db.commit()

cols5 = "`,`".join(level3.columns)

# Insert DataFrame recrds one by one.
for i,row in level3.iterrows():
    sql5 = "INSERT INTO `TB_26_FRTB_SENSITIVITY_DECOMP_PORTFOLIO` (`" +cols5 + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql5, tuple(row))
db.commit()

cursor.close()
db.close()


# In[ ]:


end_time = time.time()


# In[ ]:


print("Execution time is " , end_time-start_time , "seconds.")

