#!/usr/bin/env python
# coding: utf-8


# In[50]:
import numpy as np
import pandas as pd
import pymysql
import FRTB_module
from datetime import *
import time

import warnings
warnings.filterwarnings('ignore')

# #### params (from excel file)



# method for getting params
def getParam(_type):
    
    params=pd.ExcelFile('params.xlsx')
    
    if _type == 'High_Multipler':
        Param = 1.25
    if _type == 'Low_Multipler1':
        Param = 2
    if _type == 'Low_Multipler2':
        Param = 0.75
    
    if _type == 'GIRR_Weights':
        Param = params.parse('GIRR_Weights')
    if _type == 'GIRR_Weights_Infl':
        Param = 0.016
    if _type == 'GIRR_Weights_Basis':
        Param = 0.016
    if _type == 'GIRR_Rho':
        Param = params.parse('GIRR_Rho')
    if _type == 'GIRR_Diff_Mlt':
        Param = 0.999
    if _type == 'GIRR_Infl_Mlt':
        Param = 0.4
    if _type == 'GIRR_Cross_Mlt':
        Param = 0
    if _type == 'GIRR_Gamma':
        Param = 0.5
    if _type == 'GIRR_vega_rw':
        Param = 1
    
    if _type == 'CSR_Weights':
        Param = params.parse('CSR_Weights')
    if _type == 'CSR_Rho_Name':
        Param = 0.35
    if _type == 'CSR_Rho_Tenor':
        Param = 0.65
    if _type == 'CSR_Rho_Basis':
        Param = 0.999
    if _type == 'CSR_Gamma':
        CSR_Gamma = params.parse('CSR_Gamma')
        CSR_Gamma['Gamma_bc'] = CSR_Gamma['Gamma_bc_Rating']*CSR_Gamma['Gamma_bc_Sector']
        CSR_Gamma['Bucket_b']=CSR_Gamma['Bucket_b'].astype(str)
        CSR_Gamma['Bucket_c']=CSR_Gamma['Bucket_c'].astype(str)
        Param = CSR_Gamma
    if _type == 'CSR_vega_rw':
        Param = 1
    
    if _type == 'CSRNC_Weights':
        Param = params.parse('CSRNonCTP_Weights')
        Param['RISK_FACTOR_BUCKET']=Param['RISK_FACTOR_BUCKET'].astype(str)
    if _type == 'CSRNC_Rho_Tranch':
        Param = 0.4
    if _type == 'CSRNC_Rho_Tenor':
        Param= 0.8
    if _type == 'CSRNC_Rho_Basis':
        Param = 0.999
    if _type == 'CSRNC_Gamma':
        CSRNC_Gamma = params.parse('CSRNonCTP_Gamma')
        CSRNC_Gamma['Bucket_b']=CSRNC_Gamma['Bucket_b'].astype(str)
        CSRNC_Gamma['Bucket_c']=CSRNC_Gamma['Bucket_c'].astype(str)
        Param = CSRNC_Gamma
    if _type == 'CSRNC_vega_rw':
        Param = 1
        
    if _type == 'Equity_Weights':
        Param = params.parse('Equity_Weights')
    if _type == 'Equity_Rho':
        Param = params.parse('Equity_Rho')
    if _type == 'Equity_Rho_Diff':
        Param = 0.999
    if _type == 'Equity_Gamma':
        Param = params.parse('Equity_Gamma')
    if _type == 'Equity_Big_RW':
        Param = 0.55*np.sqrt(20/10)
    if _type == 'Equity_Small_RW':
        Param = 1
    
    if _type == 'CMTY_Weights':
        Param = params.parse('Commodity_Weights')
        Param['RISK_FACTOR_BUCKET']=Param['RISK_FACTOR_BUCKET'].astype(str)
    if _type == 'CMTY_Rho_Cty':
        CMTY_Rho_Cty = params.parse('Commodity_Rho')
        CMTY_Rho_Cty['RISK_FACTOR_BUCKET']=CMTY_Rho_Cty['RISK_FACTOR_BUCKET'].astype(str)
        Param = CMTY_Rho_Cty
    if _type == 'CMTY_Rho_Tenor':
        Param = 0.99 #time differs
    if _type == 'CMTY_Rho_Basis':
        Param = 0.999 #location differs
    if _type == 'CMTY_Gamma':
        CMTY_Gamma = params.parse('Commodity_Gamma')
        CMTY_Gamma['Bucket_b']=CMTY_Gamma['Bucket_b'].astype(str)
        CMTY_Gamma['Bucket_c']=CMTY_Gamma['Bucket_c'].astype(str)
        Param = CMTY_Gamma
    if _type == 'CMTY_vega_rw':
        Param = 1
    
    if _type == 'FX_Weights':
        Param = params.parse('FX_Weights')
    if _type == 'FX_Gamma':
        Param = 0.6
    if _type == 'FX_vega_rw':
        Param = 1
    #else:
    #    Param = np.nan
    return Param
    


# #### GIRR

# ##### GIRR_Delta

# In[22]:


def GIRR_Delta(Raw_Data):
    #get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    
    GIRR_Weights = getParam('GIRR_Weights')
    GIRR_Weights_Infl = getParam('GIRR_Weights_Infl')
    GIRR_Weights_Basis = getParam('GIRR_Weights_Basis')
    GIRR_Rho = getParam('GIRR_Rho')
    GIRR_Diff_Mlt = getParam('GIRR_Diff_Mlt')
    GIRR_Infl_Mlt = getParam('GIRR_Infl_Mlt')
    GIRR_Cross_Mlt = getParam('GIRR_Cross_Mlt')
    GIRR_Gamma = getParam('GIRR_Gamma')
    GIRR_LH = getParam('GIRR_LH')
    GIRR_vega_rw = getParam('GIRR_vega_rw')
    
    GIRR_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='GIRR')]

    GIRR_Position = GIRR_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_CLASS',
                                  'RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    GIRR_Position = GIRR_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2',
                                           'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE',
                                           'SENSITIVITY_TYPE']
                                          ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    GIRR_delta = GIRR_Position[(GIRR_Position['SENSITIVITY_TYPE']=='Delta')]
    
    GIRR_delta = GIRR_delta.merge(GIRR_Weights,on='RISK_FACTOR_VERTEX_1',how='left')
    GIRR_delta.loc[GIRR_delta.RISK_FACTOR_TYPE=='Basis','RW']=GIRR_Weights_Basis
    GIRR_delta.loc[GIRR_delta.RISK_FACTOR_TYPE=='Inflation','RW']=GIRR_Weights_Infl
    GIRR_delta = GIRR_delta.rename({'RW':'RISKWEIGHT'},axis=1)
    GIRR_delta['WEIGHTED_SENSITIVITY'] = GIRR_delta['SENSITIVITY_VAL_RPT_CURR_CNY']*GIRR_delta['RISKWEIGHT']

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

    GIRR_delta_kl = GIRR_delta_kl.merge(GIRR_Rho,on=['RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_VERTEX_1_L'],how='left')
    GIRR_delta_kl.rename(columns={'Rho_KL':'Rho_kl_M'},inplace=True)
    GIRR_delta_kl.loc[GIRR_delta_kl.RISK_FACTOR_ID_K!=GIRR_delta_kl.RISK_FACTOR_ID_L,'Rho_kl_M']=GIRR_delta_kl['Rho_kl_M']*GIRR_Diff_Mlt
    GIRR_delta_kl.loc[((GIRR_delta_kl.RISK_FACTOR_TYPE_K=='basis') | (GIRR_delta_kl.RISK_FACTOR_TYPE_L=='basis')) 
                      & (GIRR_delta_kl.RISK_FACTOR_TYPE_K!=GIRR_delta_kl.RISK_FACTOR_TYPE_L)
                      ,'Rho_kl_M'] = GIRR_Cross_Mlt

    GIRR_delta_kl['Rho_kl_H'] = np.minimum(1, GIRR_delta_kl['Rho_kl_M']*High_Multipler)
    GIRR_delta_kl['Rho_kl_L'] = np.maximum((Low_Multipler1*GIRR_delta_kl['Rho_kl_M']-1),(Low_Multipler2*GIRR_delta_kl['Rho_kl_M']))
    GIRR_delta_kl['rslt_kl_M'] = GIRR_delta_kl['WEIGHTED_SENSITIVITY_K']*GIRR_delta_kl['WEIGHTED_SENSITIVITY_L']*GIRR_delta_kl['Rho_kl_M']
    GIRR_delta_kl['rslt_kl_H'] = GIRR_delta_kl['WEIGHTED_SENSITIVITY_K']*GIRR_delta_kl['WEIGHTED_SENSITIVITY_L']*GIRR_delta_kl['Rho_kl_H']
    GIRR_delta_kl['rslt_kl_L'] = GIRR_delta_kl['WEIGHTED_SENSITIVITY_K']*GIRR_delta_kl['WEIGHTED_SENSITIVITY_L']*GIRR_delta_kl['Rho_kl_L']

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

    GIRR_delta_agg = GIRR_delta.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()

    GIRR_delta_bc = GIRR_delta_agg.rename(
                        {'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                    ).merge(GIRR_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c','WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                            ,on=['RISK_FACTOR_CLASS'],how='left')
    GIRR_delta_bc = GIRR_delta_bc.loc[(GIRR_delta_bc.Bucket_b!=GIRR_delta_bc.Bucket_c),:].reset_index(drop=True)

    GIRR_delta_bc['Gamma_bc_M']=0.5
    GIRR_delta_bc['Gamma_bc_H'] = np.minimum(1, GIRR_delta_bc['Gamma_bc_M']*High_Multipler)
    GIRR_delta_bc['Gamma_bc_L'] = np.maximum((Low_Multipler1*GIRR_delta_bc['Gamma_bc_M']-1),(Low_Multipler2*GIRR_delta_bc['Gamma_bc_M']))
    GIRR_delta_bc['rslt_bc_M']=GIRR_delta_bc.WS_b*GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_M
    GIRR_delta_bc['rslt_bc_H']=GIRR_delta_bc.WS_b*GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_H
    GIRR_delta_bc['rslt_bc_L']=GIRR_delta_bc.WS_b*GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_L

    GIRR_delta_bc['gammac_M']=GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_M
    GIRR_delta_bc['gammac_H']=GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_H
    GIRR_delta_bc['gammac_L']=GIRR_delta_bc.WS_c*GIRR_delta_bc.Gamma_bc_L

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

    GIRR_delta_agg['Sb*_M']=np.maximum(np.minimum(GIRR_delta_agg['Kb_M'],GIRR_delta_agg['Sb_M']),-GIRR_delta_agg['Kb_M'])
    GIRR_delta_agg['Sb*_H']=np.maximum(np.minimum(GIRR_delta_agg['Kb_H'],GIRR_delta_agg['Sb_H']),-GIRR_delta_agg['Kb_H'])
    GIRR_delta_agg['Sb*_L']=np.maximum(np.minimum(GIRR_delta_agg['Kb_L'],GIRR_delta_agg['Sb_L']),-GIRR_delta_agg['Kb_L'])

    GIRR_delta_bc=GIRR_delta_bc.merge(
        GIRR_delta_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
        ,left_on=['Bucket_b'],right_on=['RISK_FACTOR_BUCKET'],how='left')

    GIRR_delta_bc=GIRR_delta_bc.merge(
        GIRR_delta_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
        ,left_on=['Bucket_c'],right_on=['RISK_FACTOR_BUCKET'],how='left')

    GIRR_delta_bc=GIRR_delta_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)

    GIRR_delta_bc['rslt_bc*_M']=GIRR_delta_bc['Sb*_M']*GIRR_delta_bc['Sc*_M']*GIRR_delta_bc['Gamma_bc_M']
    GIRR_delta_bc['rslt_bc*_H']=GIRR_delta_bc['Sb*_H']*GIRR_delta_bc['Sc*_H']*GIRR_delta_bc['Gamma_bc_H']
    GIRR_delta_bc['rslt_bc*_L']=GIRR_delta_bc['Sb*_L']*GIRR_delta_bc['Sc*_L']*GIRR_delta_bc['Gamma_bc_L']

    girrd = pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=['0'])

    girrd_M_est=sum(GIRR_delta_agg['Kb_M^2'])+sum(GIRR_delta_bc['rslt_bc_M'])
    girrd_M_1=np.sqrt(sum(GIRR_delta_agg['Kb_M^2'])+sum(GIRR_delta_bc['rslt_bc_M']))
    girrd_M_2=np.sqrt(sum(GIRR_delta_agg['Kb_M^2'])+sum(GIRR_delta_bc['rslt_bc*_M']))

    girrd_H_est=sum(GIRR_delta_agg['Kb_H^2'])+sum(GIRR_delta_bc['rslt_bc_H'])
    girrd_H_1=np.sqrt(sum(GIRR_delta_agg['Kb_H^2'])+sum(GIRR_delta_bc['rslt_bc_H']))
    girrd_H_2=np.sqrt(sum(GIRR_delta_agg['Kb_H^2'])+sum(GIRR_delta_bc['rslt_bc*_H']))

    girrd_L_est=sum(GIRR_delta_agg['Kb_L^2'])+sum(GIRR_delta_bc['rslt_bc_L'])
    girrd_L_1=np.sqrt(sum(GIRR_delta_agg['Kb_L^2'])+sum(GIRR_delta_bc['rslt_bc_L']))
    girrd_L_2=np.sqrt(sum(GIRR_delta_agg['Kb_L^2'])+sum(GIRR_delta_bc['rslt_bc*_L']))

    girrd['RISK_FACTOR_CLASS']='GIRR'
    girrd['SENS_TYPE']='DELTA'
    girrd['NORMAL']=np.where(girrd_M_est>=0,girrd_M_1,girrd_M_2)
    girrd['HIGH']=np.where(girrd_H_est>=0,girrd_H_1,girrd_H_2)
    girrd['LOW']=np.where(girrd_L_est>=0,girrd_L_1,girrd_L_2)

    girrd_1=GIRR_delta[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
    girrd_2=GIRR_delta_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                          ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()
    girrd_3=GIRR_delta_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                          ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    girrd_4=GIRR_delta_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]

    girrd_decomp=girrd_1.merge(girrd_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET']
                              ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET'],how='left')\
    .merge(girrd_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(girrd_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(girrd,on=['RISK_FACTOR_CLASS'],how='left')

    girrd_decomp=girrd_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','Bucket_b','SENS_TYPE'],axis=1)

    girrd_decomp['M_est']=girrd_M_est
    girrd_decomp['H_est']=girrd_H_est
    girrd_decomp['L_est']=girrd_L_est

    #case 1
    girrd_decomp.loc[(girrd_decomp['M_est']>=0)&(girrd_decomp['Kb_M']>0),'pder_M']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_M']+girrd_decomp['gammac_M'])/girrd_decomp['NORMAL']
    girrd_decomp.loc[(girrd_decomp['H_est']>=0)&(girrd_decomp['Kb_H']>0),'pder_H']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_H']+girrd_decomp['gammac_H'])/girrd_decomp['HIGH']
    girrd_decomp.loc[(girrd_decomp['L_est']>=0)&(girrd_decomp['Kb_L']>0),'pder_L']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_L']+girrd_decomp['gammac_L'])/girrd_decomp['LOW']

    #case 2
    girrd_decomp.loc[(girrd_decomp['M_est']>=0)&(girrd_decomp['Kb_M']==0),'pder_M']=girrd_decomp['gammac_M']/girrd_decomp['NORMAL']
    girrd_decomp.loc[(girrd_decomp['H_est']>=0)&(girrd_decomp['Kb_H']==0),'pder_H']=girrd_decomp['gammac_H']/girrd_decomp['HIGH']
    girrd_decomp.loc[(girrd_decomp['L_est']>=0)&(girrd_decomp['Kb_L']==0),'pder_L']=girrd_decomp['gammac_L']/girrd_decomp['LOW']

    #case 3
    girrd_decomp.loc[(girrd_decomp['M_est']<0)&(girrd_decomp['Kb_M']>0)&(girrd_decomp['Sb*_M']==girrd_decomp['Kb_M']),'pder_M']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_M'])*(1+1/girrd_decomp['Kb_M']*girrd_decomp['gammac_M']))/girrd_decomp['NORMAL']
    girrd_decomp.loc[(girrd_decomp['H_est']<0)&(girrd_decomp['Kb_H']>0)&(girrd_decomp['Sb*_H']==girrd_decomp['Kb_H']),'pder_H']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_H'])*(1+1/girrd_decomp['Kb_H']*girrd_decomp['gammac_H']))/girrd_decomp['HIGH']
    girrd_decomp.loc[(girrd_decomp['L_est']<0)&(girrd_decomp['Kb_L']>0)&(girrd_decomp['Sb*_L']==girrd_decomp['Kb_L']),'pder_L']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_L'])*(1+1/girrd_decomp['Kb_L']*girrd_decomp['gammac_L']))/girrd_decomp['LOW']

    #case 4
    girrd_decomp.loc[(girrd_decomp['M_est']<0)&(girrd_decomp['Kb_M']>0)&(girrd_decomp['Sb*_M']+girrd_decomp['Kb_M']==0),'pder_M']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_M'])*(1-1/girrd_decomp['Kb_M']*girrd_decomp['gammac_M']))/girrd_decomp['NORMAL']
    girrd_decomp.loc[(girrd_decomp['H_est']<0)&(girrd_decomp['Kb_H']>0)&(girrd_decomp['Sb*_H']+girrd_decomp['Kb_H']==0),'pder_H']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_H'])*(1-1/girrd_decomp['Kb_H']*girrd_decomp['gammac_H']))/girrd_decomp['HIGH']
    girrd_decomp.loc[(girrd_decomp['L_est']<0)&(girrd_decomp['Kb_L']>0)&(girrd_decomp['Sb*_L']+girrd_decomp['Kb_L']==0),'pder_L']=((girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_L'])*(1-1/girrd_decomp['Kb_L']*girrd_decomp['gammac_L']))/girrd_decomp['LOW']

    #case 5
    girrd_decomp.loc[(girrd_decomp['M_est']<0)&(girrd_decomp['Kb_M']>0)&(abs(girrd_decomp['Sb*_M'])!=abs(girrd_decomp['Kb_M'])),'pder_M']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_M']+girrd_decomp['gammac_M'])/girrd_decomp['NORMAL']
    girrd_decomp.loc[(girrd_decomp['H_est']<0)&(girrd_decomp['Kb_H']>0)&(abs(girrd_decomp['Sb*_H'])!=abs(girrd_decomp['Kb_H'])),'pder_H']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_H']+girrd_decomp['gammac_H'])/girrd_decomp['HIGH']
    girrd_decomp.loc[(girrd_decomp['L_est']<0)&(girrd_decomp['Kb_L']>0)&(abs(girrd_decomp['Sb*_L'])!=abs(girrd_decomp['Kb_L'])),'pder_L']=(girrd_decomp['WEIGHTED_SENSITIVITY']+girrd_decomp['rhol_L']+girrd_decomp['gammac_L'])/girrd_decomp['LOW']

    #case 6
    girrd_decomp.loc[(girrd_decomp['M_est']<0)&(girrd_decomp['Kb_M']==0),'pder_M']=0
    girrd_decomp.loc[(girrd_decomp['H_est']<0)&(girrd_decomp['Kb_H']==0),'pder_H']=0
    girrd_decomp.loc[(girrd_decomp['L_est']<0)&(girrd_decomp['Kb_L']==0),'pder_L']=0

    girrd_decomp=girrd_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]

    girrd_decomp_rslt=GIRR_delta.merge(girrd_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'],how='left')
    
    return GIRR_delta, GIRR_delta_agg, girrd, girrd_decomp_rslt


# ##### GIRR_Vega 

# In[37]:


def GIRR_Vega(Raw_Data):
    #get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    
    GIRR_Weights = getParam('GIRR_Weights')
    GIRR_Weights_Infl = getParam('GIRR_Weights_Infl')
    GIRR_Weights_Basis = getParam('GIRR_Weights_Basis')
    GIRR_Rho = getParam('GIRR_Rho')
    GIRR_Diff_Mlt = getParam('GIRR_Diff_Mlt')
    GIRR_Infl_Mlt = getParam('GIRR_Infl_Mlt')
    GIRR_Cross_Mlt = getParam('GIRR_Cross_Mlt')
    GIRR_Gamma = getParam('GIRR_Gamma')
    GIRR_LH = getParam('GIRR_LH')
    GIRR_vega_rw = getParam('GIRR_vega_rw')
    
    GIRR_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='GIRR')]
    GIRR_Position = GIRR_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_CLASS',
                                  'RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    GIRR_Position = GIRR_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2',
                                           'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE',
                                           'SENSITIVITY_TYPE']
                                          ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    GIRR_vega = GIRR_Position[(GIRR_Position['SENSITIVITY_TYPE']=='Vega')]
    GIRR_vega = GIRR_vega.assign(RISKWEIGHT = GIRR_vega_rw)
    GIRR_vega = GIRR_vega.assign(WEIGHTED_SENSITIVITY = GIRR_vega['SENSITIVITY_VAL_RPT_CURR_CNY']*GIRR_vega['RISKWEIGHT'])
    
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

    GIRR_vega_kl['Rho_kl_opt_mat_M'] = np.exp(
        -0.01*abs(
            GIRR_vega_kl['RISK_FACTOR_VERTEX_1_K']-GIRR_vega_kl['RISK_FACTOR_VERTEX_1_L']
        )/np.minimum(GIRR_vega_kl['RISK_FACTOR_VERTEX_1_K'],GIRR_vega_kl['RISK_FACTOR_VERTEX_1_L']))

    GIRR_vega_kl['Rho_kl_und_mat_M'] = np.exp(
        -0.01*abs(
            GIRR_vega_kl['RISK_FACTOR_VERTEX_2_K']-GIRR_vega_kl['RISK_FACTOR_VERTEX_2_L']
        )/np.minimum(GIRR_vega_kl['RISK_FACTOR_VERTEX_2_K'],GIRR_vega_kl['RISK_FACTOR_VERTEX_2_L']))

    GIRR_vega_kl['Rho_kl_M']=np.minimum((GIRR_vega_kl['Rho_kl_opt_mat_M']*GIRR_vega_kl['Rho_kl_und_mat_M']),1)
    GIRR_vega_kl['rslt_kl_M']=GIRR_vega_kl['Rho_kl_M']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_K']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_L']
    GIRR_vega_kl['Rho_kl_H']=np.minimum(1,High_Multipler*GIRR_vega_kl['Rho_kl_M'])
    GIRR_vega_kl['rslt_kl_H']=GIRR_vega_kl['Rho_kl_H']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_K']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_L']
    GIRR_vega_kl['Rho_kl_L']=np.maximum(Low_Multipler1*GIRR_vega_kl['Rho_kl_M']-1,Low_Multipler2*GIRR_vega_kl['Rho_kl_M'])
    GIRR_vega_kl['rslt_kl_L']=GIRR_vega_kl['Rho_kl_L']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_K']*GIRR_vega_kl['WEIGHTED_SENSITIVITY_L']

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

    GIRR_vega_agg = GIRR_vega.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()

    GIRR_vega_bc = GIRR_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                   ).merge(GIRR_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c','WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                           ,on='RISK_FACTOR_CLASS',how='left')

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

    GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']==GIRR_vega_bc['Bucket_c'],'gammac_M']=0
    GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']!=GIRR_vega_bc['Bucket_c'],'gammac_M']=GIRR_vega_bc.WS_c*GIRR_vega_bc.Gamma_bc_M
    GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']==GIRR_vega_bc['Bucket_c'],'gammac_H']=0
    GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']!=GIRR_vega_bc['Bucket_c'],'gammac_H']=GIRR_vega_bc.WS_c*GIRR_vega_bc.Gamma_bc_H
    GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']==GIRR_vega_bc['Bucket_c'],'gammac_L']=0
    GIRR_vega_bc.loc[GIRR_vega_bc['Bucket_b']!=GIRR_vega_bc['Bucket_c'],'gammac_L']=GIRR_vega_bc.WS_c*GIRR_vega_bc.Gamma_bc_L

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

    girrv=pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])

    girrv_M_est=sum(GIRR_vega_agg['Kb_M^2'])+sum(GIRR_vega_bc['rslt_bc_M'])
    girrv_M_1=np.sqrt(sum(GIRR_vega_agg['Kb_M^2'])+sum(GIRR_vega_bc['rslt_bc_M']))
    girrv_M_2=np.sqrt(sum(GIRR_vega_agg['Kb_M^2'])+sum(GIRR_vega_bc['rslt_bc*_M']))

    girrv_H_est=sum(GIRR_vega_agg['Kb_H^2'])+sum(GIRR_vega_bc['rslt_bc_H'])
    girrv_H_1=np.sqrt(sum(GIRR_vega_agg['Kb_H^2'])+sum(GIRR_vega_bc['rslt_bc_H']))
    girrv_H_2=np.sqrt(sum(GIRR_vega_agg['Kb_H^2'])+sum(GIRR_vega_bc['rslt_bc*_H']))

    girrv_L_est=sum(GIRR_vega_agg['Kb_L^2'])+sum(GIRR_vega_bc['rslt_bc_L'])
    girrv_L_1=np.sqrt(sum(GIRR_vega_agg['Kb_L^2'])+sum(GIRR_vega_bc['rslt_bc_L']))
    girrv_L_2=np.sqrt(sum(GIRR_vega_agg['Kb_L^2'])+sum(GIRR_vega_bc['rslt_bc*_L']))

    girrv['RISK_FACTOR_CLASS']='GIRR'
    girrv['SENS_TYPE']='VEGA'
    girrv['NORMAL']=np.where(girrv_M_est>=0,girrv_M_1,girrv_M_2)
    girrv['HIGH']=np.where(girrv_H_est>=0,girrv_H_1,girrv_H_2)
    girrv['LOW']=np.where(girrv_L_est>=0,girrv_L_1,girrv_L_2)

    girrv_1=GIRR_vega[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
    girrv_2=GIRR_vega_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_VERTEX_2_K','RISK_FACTOR_BUCKET']
                                 ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()
    girrv_3=GIRR_vega_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                                 ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    girrv_4=GIRR_vega_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]

    girrv_decomp=girrv_1.merge(girrv_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_BUCKET']
                               ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_VERTEX_2_K','RISK_FACTOR_BUCKET']
                               ,how='left')\
    .merge(girrv_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(girrv_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(girrv,on=['RISK_FACTOR_CLASS'],how='left')

    girrv_decomp=girrv_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_VERTEX_2_K','Bucket_b','SENS_TYPE'],axis=1)

    girrv_decomp['M_est']=girrv_M_est
    girrv_decomp['H_est']=girrv_H_est
    girrv_decomp['L_est']=girrv_L_est

    #case 1
    girrv_decomp.loc[(girrv_decomp['M_est']>=0)&(girrv_decomp['Kb_M']>0),'pder_M']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_M']+girrv_decomp['gammac_M'])/girrv_decomp['NORMAL']
    girrv_decomp.loc[(girrv_decomp['H_est']>=0)&(girrv_decomp['Kb_H']>0),'pder_H']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_H']+girrv_decomp['gammac_H'])/girrv_decomp['HIGH']
    girrv_decomp.loc[(girrv_decomp['L_est']>=0)&(girrv_decomp['Kb_L']>0),'pder_L']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_L']+girrv_decomp['gammac_L'])/girrv_decomp['LOW']

    #case 2
    girrv_decomp.loc[(girrv_decomp['M_est']>=0)&(girrv_decomp['Kb_M']==0),'pder_M']=girrv_decomp['gammac_M']/girrv_decomp['NORMAL']
    girrv_decomp.loc[(girrv_decomp['H_est']>=0)&(girrv_decomp['Kb_H']==0),'pder_H']=girrv_decomp['gammac_H']/girrv_decomp['HIGH']
    girrv_decomp.loc[(girrv_decomp['L_est']>=0)&(girrv_decomp['Kb_L']==0),'pder_L']=girrv_decomp['gammac_L']/girrv_decomp['LOW']

    #case 3
    girrv_decomp.loc[(girrv_decomp['M_est']<0)&(girrv_decomp['Kb_M']>0)&(girrv_decomp['Sb*_M']==girrv_decomp['Kb_M']),'pder_M']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_M'])*(1+1/girrv_decomp['Kb_M']*girrv_decomp['gammac_M']))/girrv_decomp['NORMAL']
    girrv_decomp.loc[(girrv_decomp['H_est']<0)&(girrv_decomp['Kb_H']>0)&(girrv_decomp['Sb*_H']==girrv_decomp['Kb_H']),'pder_H']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_H'])*(1+1/girrv_decomp['Kb_H']*girrv_decomp['gammac_H']))/girrv_decomp['HIGH']
    girrv_decomp.loc[(girrv_decomp['L_est']<0)&(girrv_decomp['Kb_L']>0)&(girrv_decomp['Sb*_L']==girrv_decomp['Kb_L']),'pder_L']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_L'])*(1+1/girrv_decomp['Kb_L']*girrv_decomp['gammac_L']))/girrv_decomp['LOW']

    #case 4
    girrv_decomp.loc[(girrv_decomp['M_est']<0)&(girrv_decomp['Kb_M']>0)&(girrv_decomp['Sb*_M']+girrv_decomp['Kb_M']==0),'pder_M']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_M'])*(1-1/girrv_decomp['Kb_M']*girrv_decomp['gammac_M']))/girrv_decomp['NORMAL']
    girrv_decomp.loc[(girrv_decomp['H_est']<0)&(girrv_decomp['Kb_H']>0)&(girrv_decomp['Sb*_H']+girrv_decomp['Kb_H']==0),'pder_H']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_H'])*(1-1/girrv_decomp['Kb_H']*girrv_decomp['gammac_H']))/girrv_decomp['HIGH']
    girrv_decomp.loc[(girrv_decomp['L_est']<0)&(girrv_decomp['Kb_L']>0)&(girrv_decomp['Sb*_L']+girrv_decomp['Kb_L']==0),'pder_L']=((girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_L'])*(1-1/girrv_decomp['Kb_L']*girrv_decomp['gammac_L']))/girrv_decomp['LOW']

    #case 5
    girrv_decomp.loc[(girrv_decomp['M_est']<0)&(girrv_decomp['Kb_M']>0)&(abs(girrv_decomp['Sb*_M'])!=abs(girrv_decomp['Kb_M'])),'pder_M']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_M']+girrv_decomp['gammac_M'])/girrv_decomp['NORMAL']
    girrv_decomp.loc[(girrv_decomp['H_est']<0)&(girrv_decomp['Kb_H']>0)&(abs(girrv_decomp['Sb*_H'])!=abs(girrv_decomp['Kb_H'])),'pder_H']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_H']+girrv_decomp['gammac_H'])/girrv_decomp['HIGH']
    girrv_decomp.loc[(girrv_decomp['L_est']<0)&(girrv_decomp['Kb_L']>0)&(abs(girrv_decomp['Sb*_L'])!=abs(girrv_decomp['Kb_L'])),'pder_L']=(girrv_decomp['WEIGHTED_SENSITIVITY']+girrv_decomp['rhol_L']+girrv_decomp['gammac_L'])/girrv_decomp['LOW']

    #case 6
    girrv_decomp.loc[(girrv_decomp['M_est']<0)&(girrv_decomp['Kb_M']==0),'pder_M']=0
    girrv_decomp.loc[(girrv_decomp['H_est']<0)&(girrv_decomp['Kb_H']==0),'pder_H']=0
    girrv_decomp.loc[(girrv_decomp['L_est']<0)&(girrv_decomp['Kb_L']==0),'pder_L']=0

    girrv_decomp=girrv_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]

    girrv_decomp_rslt=GIRR_vega.merge(girrv_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_BUCKET'],how='left')

    girrv_decomp_rslt=girrv_decomp_rslt.fillna({'pder_M':0,'pder_H':0,'pder_L':0})

    return GIRR_vega, GIRR_vega_agg, girrv, girrv_decomp_rslt


# ##### GIRR_Curvature

# In[39]:


def GIRR_Curvature(Raw_Data):
    #get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    
    GIRR_Weights = getParam('GIRR_Weights')
    GIRR_Weights_Infl = getParam('GIRR_Weights_Infl')
    GIRR_Weights_Basis = getParam('GIRR_Weights_Basis')
    GIRR_Rho = getParam('GIRR_Rho')
    GIRR_Diff_Mlt = getParam('GIRR_Diff_Mlt')
    GIRR_Infl_Mlt = getParam('GIRR_Infl_Mlt')
    GIRR_Cross_Mlt = getParam('GIRR_Cross_Mlt')
    GIRR_Gamma = getParam('GIRR_Gamma')
    GIRR_LH = getParam('GIRR_LH')
    GIRR_vega_rw = getParam('GIRR_vega_rw')
    
    GIRR_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='GIRR')]
    GIRR_Position = GIRR_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2','RISK_FACTOR_CLASS',
                                  'RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    GIRR_Position = GIRR_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2',
                                           'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE',
                                           'SENSITIVITY_TYPE']
                                          ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()
    GIRR_curvature = GIRR_Position.query('SENSITIVITY_TYPE=="Curvature Up"|SENSITIVITY_TYPE=="Curvature Down"')

    GIRR_curvature = GIRR_curvature.assign(max_0_square=np.square(np.maximum(GIRR_curvature['SENSITIVITY_VAL_RPT_CURR_CNY'],0)))
    GIRR_curvature = GIRR_curvature.assign(WEIGHTED_SENSITIVITY=GIRR_curvature['SENSITIVITY_VAL_RPT_CURR_CNY'])

    GIRR_curvature_agg = GIRR_curvature.groupby(
        ['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],dropna=False
    ).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum','max_0_square':'sum'}).reset_index()

    GIRR_curvature_agg['max_0_k']=np.sqrt(GIRR_curvature_agg['max_0_square'])

    GIRR_curvature_agg=GIRR_curvature_agg.pivot(index=('RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET')
                             ,columns='SENSITIVITY_TYPE')

    GIRR_curvature_agg.columns=['/'.join(i) for i in GIRR_curvature_agg.columns]
    GIRR_curvature_agg=GIRR_curvature_agg.reset_index()

    GIRR_curvature_agg['Kb+_M']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Up'])))
    GIRR_curvature_agg['Kb-_M']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Down'])))
    GIRR_curvature_agg['Kb_M']=np.maximum(GIRR_curvature_agg['Kb+_M'],GIRR_curvature_agg['Kb-_M'])
    GIRR_curvature_agg['Kb_M^2']=np.square(GIRR_curvature_agg['Kb_M'])
    GIRR_curvature_agg['Sb_M']=np.select([(GIRR_curvature_agg['Kb_M'] == GIRR_curvature_agg['Kb+_M']),
                                          (GIRR_curvature_agg['Kb_M'] != GIRR_curvature_agg['Kb+_M'])],
                                         [(GIRR_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (GIRR_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    GIRR_curvature_agg['Kb+_H']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Up'])))
    GIRR_curvature_agg['Kb-_H']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Down'])))
    GIRR_curvature_agg['Kb_H']=np.maximum(GIRR_curvature_agg['Kb+_H'],GIRR_curvature_agg['Kb-_H'])
    GIRR_curvature_agg['Kb_H^2']=np.square(GIRR_curvature_agg['Kb_H'])
    GIRR_curvature_agg['Sb_H']=np.select([(GIRR_curvature_agg['Kb_H'] == GIRR_curvature_agg['Kb+_H']),
                                          (GIRR_curvature_agg['Kb_H'] != GIRR_curvature_agg['Kb+_H'])],
                                         [(GIRR_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (GIRR_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    GIRR_curvature_agg['Kb+_L']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Up'])))
    GIRR_curvature_agg['Kb-_L']=np.sqrt(np.maximum(0,(GIRR_curvature_agg['max_0_square/Curvature Down'])))
    GIRR_curvature_agg['Kb_L']=np.maximum(GIRR_curvature_agg['Kb+_L'],GIRR_curvature_agg['Kb-_L'])
    GIRR_curvature_agg['Kb_L^2']=np.square(GIRR_curvature_agg['Kb_L'])
    GIRR_curvature_agg['Sb_L']=np.select([(GIRR_curvature_agg['Kb_L'] == GIRR_curvature_agg['Kb+_L']),
                                          (GIRR_curvature_agg['Kb_L'] != GIRR_curvature_agg['Kb+_L'])],
                                         [(GIRR_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (GIRR_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    GIRR_curvature_agg['max']=np.select([(GIRR_curvature_agg['Kb_M'] == GIRR_curvature_agg['Kb+_M']),
                                         (GIRR_curvature_agg['Kb_M'] != GIRR_curvature_agg['Kb+_M'])],
                                        [(GIRR_curvature_agg['max_0_k/Curvature Up']),
                                         (GIRR_curvature_agg['max_0_k/Curvature Down'])])

    GIRR_curvature_agg['sign']=np.select([(GIRR_curvature_agg['Kb_M'] == GIRR_curvature_agg['Kb+_M']),
                                          (GIRR_curvature_agg['Kb_M'] != GIRR_curvature_agg['Kb+_M'])],
                                         ['Curvature Up','Curvature Down'])

    GIRR_curvature_bc=GIRR_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Sb_M']]
    GIRR_curvature_bc=GIRR_curvature_bc.rename(
        {'Sb_M':'Sb','RISK_FACTOR_BUCKET':'Bucket_b'},axis=1
    ).merge(GIRR_curvature_bc.rename(
        {'Sb_M':'Sc','RISK_FACTOR_BUCKET':'Bucket_c'},axis=1
    ),on=['RISK_FACTOR_CLASS'],how='left')
    #GIRR_curvature_bc=GIRR_curvature_bc[(GIRR_curvature_bc['Bucket_b']!=GIRR_curvature_bc['Bucket_c'])]

    GIRR_curvature_bc.loc[(GIRR_curvature_bc['Sb']<0) & (GIRR_curvature_bc['Sc']<0),'Psi']=0
    GIRR_curvature_bc.loc[(GIRR_curvature_bc['Sb']>=0) | (GIRR_curvature_bc['Sc']>=0),'Psi']=1
    
    GIRR_curvature_bc.loc[(GIRR_curvature_bc['Bucket_b']!=GIRR_curvature_bc['Bucket_c']),'Gamma_bc']=GIRR_Gamma
    GIRR_curvature_bc.loc[(GIRR_curvature_bc['Bucket_b']==GIRR_curvature_bc['Bucket_c']),'Gamma_bc']=0
    
    GIRR_curvature_bc['Gamma_bc_M']=np.square(GIRR_curvature_bc['Gamma_bc'])

    GIRR_curvature_bc['rslt_bc_M']=GIRR_curvature_bc['Gamma_bc_M']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sb']*GIRR_curvature_bc['Sc']
    GIRR_curvature_bc['Gamma_bc_H']=np.square(np.minimum(1,GIRR_curvature_bc['Gamma_bc']*High_Multipler))
    GIRR_curvature_bc['Gamma_bc_L']=np.square(np.maximum((Low_Multipler1*GIRR_curvature_bc['Gamma_bc']-1),(Low_Multipler2*GIRR_curvature_bc['Gamma_bc'])))
    GIRR_curvature_bc['rslt_bc_H']=GIRR_curvature_bc['Gamma_bc_H']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sb']*GIRR_curvature_bc['Sc']
    GIRR_curvature_bc['rslt_bc_L']=GIRR_curvature_bc['Gamma_bc_L']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sb']*GIRR_curvature_bc['Sc']

    GIRR_curvature_bc['gammac_M']=GIRR_curvature_bc['Gamma_bc_M']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sc']
    GIRR_curvature_bc['gammac_H']=GIRR_curvature_bc['Gamma_bc_H']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sc']
    GIRR_curvature_bc['gammac_L']=GIRR_curvature_bc['Gamma_bc_L']*GIRR_curvature_bc['Psi']*GIRR_curvature_bc['Sc']

    girrc_M_est=sum(GIRR_curvature_agg['Kb_M^2'])+sum(GIRR_curvature_bc['rslt_bc_M'])
    girrc_H_est=sum(GIRR_curvature_agg['Kb_H^2'])+sum(GIRR_curvature_bc['rslt_bc_H'])
    girrc_L_est=sum(GIRR_curvature_agg['Kb_L^2'])+sum(GIRR_curvature_bc['rslt_bc_L'])

    girrc_M = np.sqrt(np.maximum(0,sum(GIRR_curvature_agg['Kb_M^2'])+sum(GIRR_curvature_bc['rslt_bc_M'])))
    girrc_H = np.sqrt(np.maximum(0,sum(GIRR_curvature_agg['Kb_H^2'])+sum(GIRR_curvature_bc['rslt_bc_H'])))
    girrc_L = np.sqrt(np.maximum(0,sum(GIRR_curvature_agg['Kb_L^2'])+sum(GIRR_curvature_bc['rslt_bc_L'])))

    girrc=pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])

    girrc['RISK_FACTOR_CLASS']='GIRR'
    girrc['SENS_TYPE']='CURVATURE'
    girrc['NORMAL']=girrc_M
    girrc['HIGH']=girrc_H
    girrc['LOW']=girrc_L

    girrc_1=GIRR_curvature[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]
    girrc_3=GIRR_curvature_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                                      ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    girrc_4=GIRR_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','max','sign']]
    
    girrc_decomp=girrc_1.merge(girrc_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET']
                               ,right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(girrc_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(girrc,on=['RISK_FACTOR_CLASS'],how='left')

    girrc_decomp=girrc_decomp.drop(['Bucket_b','SENS_TYPE'],axis=1)

    girrc_decomp['M_est']=girrc_M_est
    girrc_decomp['H_est']=girrc_H_est
    girrc_decomp['L_est']=girrc_L_est

    girrc_decomp=girrc_decomp[(girrc_decomp.SENSITIVITY_TYPE==girrc_decomp.sign)]

    #case 1/2
    girrc_decomp.loc[(girrc_decomp['M_est']>=0),'pder_M']=(girrc_decomp['max']+girrc_decomp['gammac_M'])/girrc_decomp['NORMAL']
    girrc_decomp.loc[(girrc_decomp['H_est']>=0),'pder_H']=(girrc_decomp['max']+girrc_decomp['gammac_H'])/girrc_decomp['HIGH']
    girrc_decomp.loc[(girrc_decomp['L_est']>=0),'pder_L']=(girrc_decomp['max']+girrc_decomp['gammac_L'])/girrc_decomp['LOW']

    #case 3 
    girrc_decomp.loc[(girrc_decomp['M_est']<0),'pder_M']=0
    girrc_decomp.loc[(girrc_decomp['H_est']<0),'pder_H']=0
    girrc_decomp.loc[(girrc_decomp['L_est']<0),'pder_L']=0

    girrc_decomp=girrc_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','pder_M','pder_H','pder_L']]

    girrc_decomp_rslt=GIRR_curvature.merge(girrc_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],how='right')

    return GIRR_curvature, GIRR_curvature_agg, girrc, girrc_decomp_rslt


# #### CSR (non-sec)

# ##### CSR_Delta

# In[42]:


def CSR_Delta(Raw_Data):
    # get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    CSR_Weights = getParam('CSR_Weights')
    CSR_Rho_Name = getParam('CSR_Rho_Name')
    CSR_Rho_Tenor = getParam('CSR_Rho_Tenor')
    CSR_Rho_Basis = getParam('CSR_Rho_Basis')
    CSR_Gamma = getParam('CSR_Gamma')
    CSR_LH = getParam('CSR_LH')
    CSR_vega_rw = getParam('CSR_vega_rw')
    
    CSR_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='CSR (non-sec)')]
    CSR_RawData['SEC_ISSUER'] = CSR_RawData['RISK_FACTOR_ID']

    CSR_Position=CSR_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_TYPE'
                              ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SEC_ISSUER'
                              ,'SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    CSR_Position=CSR_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_TYPE'
                                       ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SEC_ISSUER'
                                       ,'SENSITIVITY_TYPE'],dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    CSR_delta = CSR_Position[(CSR_Position['SENSITIVITY_TYPE']=='Delta')]
    CSR_delta['RISK_FACTOR_BUCKET']=CSR_delta['RISK_FACTOR_BUCKET'].astype(int)
    CSR_delta = CSR_delta.merge(CSR_Weights,on='RISK_FACTOR_BUCKET',how='left')
    CSR_delta = CSR_delta.rename({'Risk_Weight':'RISKWEIGHT'},axis=1)
    CSR_delta['WEIGHTED_SENSITIVITY']=CSR_delta['SENSITIVITY_VAL_RPT_CURR_CNY']*CSR_delta['RISKWEIGHT']
    
    CSR_delta['abs_WS']=abs(CSR_delta['WEIGHTED_SENSITIVITY'])

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

    CSR_delta_kl.loc[CSR_delta_kl['ISSUER_K'] == CSR_delta_kl['ISSUER_L'], 'Rho_name'] = 1
    CSR_delta_kl.loc[CSR_delta_kl['ISSUER_K'] != CSR_delta_kl['ISSUER_L'], 'Rho_name'] = CSR_Rho_Name
    CSR_delta_kl.loc[CSR_delta_kl['RISK_FACTOR_VERTEX_1_K'] == CSR_delta_kl['RISK_FACTOR_VERTEX_1_L'], 'Rho_tenor'] = 1
    CSR_delta_kl.loc[CSR_delta_kl['RISK_FACTOR_VERTEX_1_K'] != CSR_delta_kl['RISK_FACTOR_VERTEX_1_L'], 'Rho_tenor'] = CSR_Rho_Tenor
    CSR_delta_kl.loc[CSR_delta_kl['RISK_FACTOR_TYPE_K'] == CSR_delta_kl['RISK_FACTOR_TYPE_L'], 'Rho_basis'] = 1
    CSR_delta_kl.loc[CSR_delta_kl['RISK_FACTOR_TYPE_K'] != CSR_delta_kl['RISK_FACTOR_TYPE_L'], 'Rho_basis'] = CSR_Rho_Basis

    CSR_delta_kl['Rho_kl_M'] = CSR_delta_kl['Rho_name']*CSR_delta_kl['Rho_tenor']*CSR_delta_kl['Rho_basis']
    CSR_delta_kl['Rho_kl_H'] = np.minimum(1,High_Multipler*CSR_delta_kl['Rho_kl_M'])
    CSR_delta_kl['Rho_kl_L'] = np.maximum(Low_Multipler1*CSR_delta_kl['Rho_kl_M']-1,Low_Multipler2*CSR_delta_kl['Rho_kl_M'])

    CSR_delta_kl['rslt_kl_M']=CSR_delta_kl['WEIGHTED_SENSITIVITY_K']*CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_M']
    CSR_delta_kl['rslt_kl_H']=CSR_delta_kl['WEIGHTED_SENSITIVITY_K']*CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_H']
    CSR_delta_kl['rslt_kl_L']=CSR_delta_kl['WEIGHTED_SENSITIVITY_K']*CSR_delta_kl['WEIGHTED_SENSITIVITY_L']*CSR_delta_kl['Rho_kl_L']

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
    
    CSR_delta_agg = CSR_delta.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET']
                                      ,dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum','abs_WS':'sum'}).reset_index()

    CSR_delta_bc = CSR_delta_agg.rename(
                        {'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b','abs_WS':'abs_WS_b'},axis=1
                    ).merge(CSR_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c','WEIGHTED_SENSITIVITY':'WS_c','abs_WS':'abs_WS_c'},axis=1)
                            ,on=['RISK_FACTOR_CLASS'],how='left')

    CSR_delta_bc = CSR_delta_bc.loc[(CSR_delta_bc.Bucket_b!=CSR_delta_bc.Bucket_c),:].reset_index(drop=True)

    CSR_delta_bc = CSR_delta_bc.merge(CSR_Gamma,on=['Bucket_b','Bucket_c'],how='left').rename({'Gamma_bc':'Gamma_bc_M'},axis=1)
    CSR_delta_bc['Gamma_bc_H'] = np.minimum(1, CSR_delta_bc['Gamma_bc_M']*High_Multipler)
    CSR_delta_bc['Gamma_bc_L'] = np.maximum((Low_Multipler1*CSR_delta_bc['Gamma_bc_M']-1),(Low_Multipler2*CSR_delta_bc['Gamma_bc_M']))

    CSR_delta_bc['rslt_bc_M']=CSR_delta_bc.WS_b*CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_M
    CSR_delta_bc['rslt_bc_H']=CSR_delta_bc.WS_b*CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_H
    CSR_delta_bc['rslt_bc_L']=CSR_delta_bc.WS_b*CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_L

    CSR_delta_bc['gammac_M']=CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_M
    CSR_delta_bc['gammac_H']=CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_H
    CSR_delta_bc['gammac_L']=CSR_delta_bc.WS_c*CSR_delta_bc.Gamma_bc_L

    CSR_delta_agg=CSR_delta_agg.merge(
        CSR_delta_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']],on=['RISK_FACTOR_BUCKET'],how='left'
    ).groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY','abs_WS']
                                          ,dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()

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

    CSR_delta_agg['Sb*_M']=np.maximum(np.minimum(CSR_delta_agg['Kb_M'],CSR_delta_agg['Sb_M']),-CSR_delta_agg['Kb_M'])
    CSR_delta_agg['Sb*_H']=np.maximum(np.minimum(CSR_delta_agg['Kb_H'],CSR_delta_agg['Sb_H']),-CSR_delta_agg['Kb_H'])
    CSR_delta_agg['Sb*_L']=np.maximum(np.minimum(CSR_delta_agg['Kb_L'],CSR_delta_agg['Sb_L']),-CSR_delta_agg['Kb_L'])

    CSR_delta_bc=CSR_delta_bc.merge(
        CSR_delta_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
        ,left_on=['Bucket_b'],right_on=['RISK_FACTOR_BUCKET'],how='left')

    CSR_delta_bc=CSR_delta_bc.merge(
        CSR_delta_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
        ,left_on=['Bucket_c'],right_on=['RISK_FACTOR_BUCKET'],how='left')

    CSR_delta_bc=CSR_delta_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)

    CSR_delta_bc['rslt_bc*_M']=CSR_delta_bc['Sb*_M']*CSR_delta_bc['Sc*_M']*CSR_delta_bc['Gamma_bc_M']
    CSR_delta_bc['rslt_bc*_H']=CSR_delta_bc['Sb*_H']*CSR_delta_bc['Sc*_H']*CSR_delta_bc['Gamma_bc_H']
    CSR_delta_bc['rslt_bc*_L']=CSR_delta_bc['Sb*_L']*CSR_delta_bc['Sc*_L']*CSR_delta_bc['Gamma_bc_L']

    csrd = pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=['0'])

    csrd_M_est=sum(CSR_delta_agg['Kb_M^2'])+sum(CSR_delta_bc['rslt_bc_M'])
    csrd_M_1=np.sqrt(sum(CSR_delta_agg['Kb_M^2'])+sum(CSR_delta_bc['rslt_bc_M']))
    csrd_M_2=np.sqrt(sum(CSR_delta_agg['Kb_M^2'])+sum(CSR_delta_bc['rslt_bc*_M']))

    csrd_H_est=sum(CSR_delta_agg['Kb_H^2'])+sum(CSR_delta_bc['rslt_bc_H'])
    csrd_H_1=np.sqrt(sum(CSR_delta_agg['Kb_H^2'])+sum(CSR_delta_bc['rslt_bc_H']))
    csrd_H_2=np.sqrt(sum(CSR_delta_agg['Kb_H^2'])+sum(CSR_delta_bc['rslt_bc*_H']))

    csrd_L_est=sum(CSR_delta_agg['Kb_L^2'])+sum(CSR_delta_bc['rslt_bc_L'])
    csrd_L_1=np.sqrt(sum(CSR_delta_agg['Kb_L^2'])+sum(CSR_delta_bc['rslt_bc_L']))
    csrd_L_2=np.sqrt(sum(CSR_delta_agg['Kb_L^2'])+sum(CSR_delta_bc['rslt_bc*_L']))

    csrd['RISK_FACTOR_CLASS']='CSR (non-sec)'
    csrd['SENS_TYPE']='DELTA'
    csrd['NORMAL']=np.where(csrd_M_est>=0,csrd_M_1,csrd_M_2)
    csrd['HIGH']=np.where(csrd_H_est>=0,csrd_H_1,csrd_H_2)
    csrd['LOW']=np.where(csrd_L_est>=0,csrd_L_1,csrd_L_2)

    csrd_1=CSR_delta[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
    csrd_2=CSR_delta_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                          ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()
    csrd_3=CSR_delta_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                          ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    csrd_4=CSR_delta_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]

    csrd_decomp=csrd_1.merge(csrd_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET']
                              ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET'],how='left')\
    .merge(csrd_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(csrd_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(csrd,on=['RISK_FACTOR_CLASS'],how='left')

    csrd_decomp=csrd_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','Bucket_b','SENS_TYPE'],axis=1)

    csrd_decomp['M_est']=csrd_M_est
    csrd_decomp['H_est']=csrd_H_est
    csrd_decomp['L_est']=csrd_L_est

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

    csrd_decomp=csrd_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','pderp_M','pderp_H','pderp_L','pderm_M','pderm_H','pderm_L']]

    csrd_decomp_rslt=CSR_delta.merge(csrd_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'],how='left')

    csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY>=0),'pder_M']=csrd_decomp_rslt.pderp_M
    csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY>=0),'pder_H']=csrd_decomp_rslt.pderp_H
    csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY>=0),'pder_L']=csrd_decomp_rslt.pderp_L
    csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY<0),'pder_M']=csrd_decomp_rslt.pderm_M
    csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY<0),'pder_H']=csrd_decomp_rslt.pderm_H
    csrd_decomp_rslt.loc[(csrd_decomp_rslt.WEIGHTED_SENSITIVITY<0),'pder_L']=csrd_decomp_rslt.pderm_L

    
    return CSR_delta, CSR_delta_agg, csrd, csrd_decomp_rslt


# ##### CSR_Vega

# In[43]:


def CSR_Vega(Raw_Data):
    # get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    CSR_Weights = getParam('CSR_Weights')
    CSR_Rho_Name = getParam('CSR_Rho_Name')
    CSR_Rho_Tenor = getParam('CSR_Rho_Tenor')
    CSR_Rho_Basis = getParam('CSR_Rho_Basis')
    CSR_Gamma = getParam('CSR_Gamma')
    CSR_LH = getParam('CSR_LH')
    CSR_vega_rw = getParam('CSR_vega_rw')
    
    CSR_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='CSR (non-sec)')]
    CSR_RawData['SEC_ISSUER'] = CSR_RawData['RISK_FACTOR_ID']

    CSR_Position=CSR_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_TYPE'
                              ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SEC_ISSUER'
                              ,'SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    CSR_Position=CSR_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_TYPE'
                                       ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SEC_ISSUER'
                                       ,'SENSITIVITY_TYPE'],dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    
    
    
    
    
    
    
    return CSR_vega, CSR_vega_agg, csrv, csrv_decomp_rslt


# ##### CSR_Curvature

# In[44]:


def CSR_Curvature(Raw_Data):
    # get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    CSR_Weights = getParam('CSR_Weights')
    CSR_Rho_Name = getParam('CSR_Rho_Name')
    CSR_Rho_Tenor = getParam('CSR_Rho_Tenor')
    CSR_Rho_Basis = getParam('CSR_Rho_Basis')
    CSR_Gamma = getParam('CSR_Gamma')
    CSR_LH = getParam('CSR_LH')
    CSR_vega_rw = getParam('CSR_vega_rw')
    
    CSR_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='CSR (non-sec)')]
    CSR_RawData['SEC_ISSUER'] = CSR_RawData['RISK_FACTOR_ID']

    CSR_Position=CSR_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_TYPE'
                              ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SEC_ISSUER'
                              ,'SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    CSR_Position=CSR_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_TYPE'
                                       ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SEC_ISSUER'
                                       ,'SENSITIVITY_TYPE'],dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    
    
    
    
    
    
    return CSR_curvature, CSR_curvature_agg, csrc, csrc_decomp_rslt


# #### CSR (non-ctp)

# ##### CSRNC_Delta

# In[46]:


def CSRNC_Delta(Raw_Data):
    # get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    CSRNC_Weights = getParam('CSRNC_Weights')
    CSRNC_Rho_Tranch = getParam('CSRNC_Rho_Tranch')
    CSRNC_Rho_Tenor = getParam('CSRNC_Rho_Tenor')
    CSRNC_Rho_Basis = getParam('CSRNC_Rho_Basis')
    CSRNC_Gamma = getParam('CSRNC_Gamma')
    CSRNC_LH = getParam('CSRNC_LH')
    CSRNC_vega_rw = getParam('CSRNC_vega_rw')
    
    CSRNC_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='CSR (non-ctp)')]
    CSRNC_RawData['SEC_TRANCHE'] = CSRNC_RawData['RISK_FACTOR_ID']

    CSRNC_Position=CSRNC_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2'
                                  ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE','SEC_TRANCHE'
                                  ,'SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    CSRNC_delta=CSRNC_Position[(CSRNC_Position['SENSITIVITY_TYPE']=='Delta')]
    CSRNC_delta = CSRNC_delta.merge(CSRNC_Weights,on='RISK_FACTOR_BUCKET',how='left').reset_index(drop=True)
    CSRNC_delta = CSRNC_delta.rename({'Risk_Weight':'RISKWEIGHT'},axis=1)
    CSRNC_delta['WEIGHTED_SENSITIVITY'] = CSRNC_delta['SENSITIVITY_VAL_RPT_CURR_CNY']*CSRNC_delta['RISKWEIGHT']

    CSRNC_delta['abs_WS']=abs(CSRNC_delta['WEIGHTED_SENSITIVITY'])

    CSRNC_delta=CSRNC_delta.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_VERTEX_2'
                                     ,'RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE','SEC_TRANCHE'
                                     ,'SENSITIVITY_TYPE'],dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'
                                                                             ,'WEIGHTED_SENSITIVITY':'sum'
                                                                             ,'abs_WS':'sum'}).reset_index()

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

    CSRNC_delta_kl.loc[CSRNC_delta_kl['SEC_TRANCHE_K']==CSRNC_delta_kl['SEC_TRANCHE_L'],'Rho_Tranch']=1
    CSRNC_delta_kl.loc[CSRNC_delta_kl['SEC_TRANCHE_K']!=CSRNC_delta_kl['SEC_TRANCHE_L'],'Rho_Tranch']=CSRNC_Rho_Tranch
    CSRNC_delta_kl.loc[CSRNC_delta_kl['RISK_FACTOR_VERTEX_1_K']==CSRNC_delta_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=1
    CSRNC_delta_kl.loc[CSRNC_delta_kl['RISK_FACTOR_VERTEX_1_K']!=CSRNC_delta_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=CSRNC_Rho_Tenor
    CSRNC_delta_kl.loc[CSRNC_delta_kl['RISK_FACTOR_TYPE_K']==CSRNC_delta_kl['RISK_FACTOR_TYPE_L'],'Rho_Basis']=1
    CSRNC_delta_kl.loc[CSRNC_delta_kl['RISK_FACTOR_TYPE_K']!=CSRNC_delta_kl['RISK_FACTOR_TYPE_L'],'Rho_Basis']=CSRNC_Rho_Basis

    CSRNC_delta_kl['Rho_kl_M']=CSRNC_delta_kl['Rho_Tranch']*CSRNC_delta_kl['Rho_Tenor']*CSRNC_delta_kl['Rho_Basis']
    CSRNC_delta_kl['Rho_kl_H']=np.minimum(1,High_Multipler*CSRNC_delta_kl['Rho_kl_M'])
    CSRNC_delta_kl['Rho_kl_L']=np.maximum(Low_Multipler1*CSRNC_delta_kl['Rho_kl_M']-1,Low_Multipler2*CSRNC_delta_kl['Rho_kl_M'])

    CSRNC_delta_kl['rslt_kl_M']=CSRNC_delta_kl['WEIGHTED_SENSITIVITY_K']*CSRNC_delta_kl['WEIGHTED_SENSITIVITY_L']*CSRNC_delta_kl['Rho_kl_M']
    CSRNC_delta_kl['rslt_kl_H']=CSRNC_delta_kl['WEIGHTED_SENSITIVITY_K']*CSRNC_delta_kl['WEIGHTED_SENSITIVITY_L']*CSRNC_delta_kl['Rho_kl_H']
    CSRNC_delta_kl['rslt_kl_L']=CSRNC_delta_kl['WEIGHTED_SENSITIVITY_K']*CSRNC_delta_kl['WEIGHTED_SENSITIVITY_L']*CSRNC_delta_kl['Rho_kl_L']

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

    CSRNC_delta_agg=CSRNC_delta.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False
                                       ).agg({'WEIGHTED_SENSITIVITY':'sum','abs_WS':'sum'}).reset_index()

    CSRNC_delta_bc=CSRNC_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b','abs_WS':'abs_WS_b'},axis=1
                                         ).merge(CSRNC_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c','WEIGHTED_SENSITIVITY':'WS_c','abs_WS':'abs_WS_c'}
                                                                        ,axis=1),on='RISK_FACTOR_CLASS',how='left').reset_index(drop=True)

    CSRNC_delta_bc = CSRNC_delta_bc.loc[(CSRNC_delta_bc.Bucket_b!=CSRNC_delta_bc.Bucket_c),:].reset_index(drop=True)

    CSRNC_delta_bc=CSRNC_delta_bc.merge(CSRNC_Gamma.rename({'Gamma_bc':'Gamma_bc_M'},axis=1),on=['Bucket_b','Bucket_c'],how='left')
    CSRNC_delta_bc['Gamma_bc_H']=np.minimum(1,High_Multipler*CSRNC_delta_bc['Gamma_bc_M'])
    CSRNC_delta_bc['Gamma_bc_L']=np.maximum(Low_Multipler1*CSRNC_delta_bc['Gamma_bc_M']-1,Low_Multipler2*CSRNC_delta_bc['Gamma_bc_M'])

    CSRNC_delta_bc['rslt_bc_M']=CSRNC_delta_bc['WS_b']*CSRNC_delta_bc['WS_c']*CSRNC_delta_bc['Gamma_bc_M']
    CSRNC_delta_bc['rslt_bc_H']=CSRNC_delta_bc['WS_b']*CSRNC_delta_bc['WS_c']*CSRNC_delta_bc['Gamma_bc_H']
    CSRNC_delta_bc['rslt_bc_L']=CSRNC_delta_bc['WS_b']*CSRNC_delta_bc['WS_c']*CSRNC_delta_bc['Gamma_bc_L']

    CSRNC_delta_bc['gammac_M']=CSRNC_delta_bc.WS_c*CSRNC_delta_bc.Gamma_bc_M
    CSRNC_delta_bc['gammac_H']=CSRNC_delta_bc.WS_c*CSRNC_delta_bc.Gamma_bc_H
    CSRNC_delta_bc['gammac_L']=CSRNC_delta_bc.WS_c*CSRNC_delta_bc.Gamma_bc_L

    CSRNC_delta_agg=CSRNC_delta_agg.merge(
        CSRNC_delta_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']],on='RISK_FACTOR_BUCKET',how='left'
    ).groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY','abs_WS']
                                          ,dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()

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

    CSRNC_delta_agg['Sb*_M']=np.maximum(np.minimum(CSRNC_delta_agg['Kb_M'],CSRNC_delta_agg['Sb_M']),-CSRNC_delta_agg['Kb_M'])
    CSRNC_delta_agg['Sb*_H']=np.maximum(np.minimum(CSRNC_delta_agg['Kb_H'],CSRNC_delta_agg['Sb_H']),-CSRNC_delta_agg['Kb_H'])
    CSRNC_delta_agg['Sb*_L']=np.maximum(np.minimum(CSRNC_delta_agg['Kb_L'],CSRNC_delta_agg['Sb_L']),-CSRNC_delta_agg['Kb_L'])

    CSRNC_delta_bc=CSRNC_delta_bc.merge(
        CSRNC_delta_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
        ,left_on='Bucket_b',right_on='RISK_FACTOR_BUCKET',how='left')

    CSRNC_delta_bc=CSRNC_delta_bc.merge(
        CSRNC_delta_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
        ,left_on='Bucket_c',right_on='RISK_FACTOR_BUCKET',how='left')

    CSRNC_delta_bc=CSRNC_delta_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)

    CSRNC_delta_bc['rslt_bc*_M']=CSRNC_delta_bc['Sb*_M']*CSRNC_delta_bc['Sc*_M']*CSRNC_delta_bc['Gamma_bc_M']
    CSRNC_delta_bc['rslt_bc*_H']=CSRNC_delta_bc['Sb*_H']*CSRNC_delta_bc['Sc*_H']*CSRNC_delta_bc['Gamma_bc_H']
    CSRNC_delta_bc['rslt_bc*_L']=CSRNC_delta_bc['Sb*_L']*CSRNC_delta_bc['Sc*_L']*CSRNC_delta_bc['Gamma_bc_L']

    csrncd = pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=['0'])

    csrncd_M_est=sum(CSRNC_delta_agg['Kb_M^2'])+sum(CSRNC_delta_bc['rslt_bc_M'])
    csrncd_M_1=np.sqrt(sum(CSRNC_delta_agg['Kb_M^2'])+sum(CSRNC_delta_bc['rslt_bc_M']))
    csrncd_M_2=np.sqrt(sum(CSRNC_delta_agg['Kb_M^2'])+sum(CSRNC_delta_bc['rslt_bc*_M']))

    csrncd_H_est=sum(CSRNC_delta_agg['Kb_H^2'])+sum(CSRNC_delta_bc['rslt_bc_H'])
    csrncd_H_1=np.sqrt(sum(CSRNC_delta_agg['Kb_H^2'])+sum(CSRNC_delta_bc['rslt_bc_H']))
    csrncd_H_2=np.sqrt(sum(CSRNC_delta_agg['Kb_H^2'])+sum(CSRNC_delta_bc['rslt_bc*_H']))

    csrncd_L_est=sum(CSRNC_delta_agg['Kb_L^2'])+sum(CSRNC_delta_bc['rslt_bc_L'])
    csrncd_L_1=np.sqrt(sum(CSRNC_delta_agg['Kb_L^2'])+sum(CSRNC_delta_bc['rslt_bc_L']))
    csrncd_L_2=np.sqrt(sum(CSRNC_delta_agg['Kb_L^2'])+sum(CSRNC_delta_bc['rslt_bc*_L']))

    csrncd['RISK_FACTOR_CLASS']='CSR (non-ctp)'
    csrncd['SENS_TYPE']='DELTA'
    csrncd['NORMAL']=np.where(csrncd_M_est>=0,csrncd_M_1,csrncd_M_2)
    csrncd['HIGH']=np.where(csrncd_H_est>=0,csrncd_H_1,csrncd_H_2)
    csrncd['LOW']=np.where(csrncd_L_est>=0,csrncd_L_1,csrncd_L_2)

    csrncd_1=CSRNC_delta[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
    csrncd_2=CSRNC_delta_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                          ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()
    csrncd_3=CSRNC_delta_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                          ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    csrncd_4=CSRNC_delta_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]
    csrncd_decomp=csrncd_1.merge(csrncd_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET']
                              ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET'],how='left')\
    .merge(csrncd_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(csrncd_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(csrncd,on=['RISK_FACTOR_CLASS'],how='left')

    csrncd_decomp=csrncd_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','Bucket_b','SENS_TYPE'],axis=1)

    csrncd_decomp['M_est']=csrncd_M_est
    csrncd_decomp['H_est']=csrncd_H_est
    csrncd_decomp['L_est']=csrncd_L_est

    #case 1
    csrncd_decomp.loc[(csrncd_decomp['M_est']>=0)&(csrncd_decomp['Kb_M']>0),'pder_M']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_M']+csrncd_decomp['gammac_M'])/csrncd_decomp['NORMAL']
    csrncd_decomp.loc[(csrncd_decomp['H_est']>=0)&(csrncd_decomp['Kb_H']>0),'pder_H']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_H']+csrncd_decomp['gammac_H'])/csrncd_decomp['HIGH']
    csrncd_decomp.loc[(csrncd_decomp['L_est']>=0)&(csrncd_decomp['Kb_L']>0),'pder_L']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_L']+csrncd_decomp['gammac_L'])/csrncd_decomp['LOW']

    #case 2
    csrncd_decomp.loc[(csrncd_decomp['M_est']>=0)&(csrncd_decomp['Kb_M']==0),'pder_M']=csrncd_decomp['gammac_M']/csrncd_decomp['NORMAL']
    csrncd_decomp.loc[(csrncd_decomp['H_est']>=0)&(csrncd_decomp['Kb_H']==0),'pder_H']=csrncd_decomp['gammac_H']/csrncd_decomp['HIGH']
    csrncd_decomp.loc[(csrncd_decomp['L_est']>=0)&(csrncd_decomp['Kb_L']==0),'pder_L']=csrncd_decomp['gammac_L']/csrncd_decomp['LOW']

    #case 3
    csrncd_decomp.loc[(csrncd_decomp['M_est']<0)&(csrncd_decomp['Kb_M']>0)&(csrncd_decomp['Sb*_M']==csrncd_decomp['Kb_M']),'pder_M']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_M'])*(1+1/csrncd_decomp['Kb_M']*csrncd_decomp['gammac_M']))/csrncd_decomp['NORMAL']
    csrncd_decomp.loc[(csrncd_decomp['H_est']<0)&(csrncd_decomp['Kb_H']>0)&(csrncd_decomp['Sb*_H']==csrncd_decomp['Kb_H']),'pder_H']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_H'])*(1+1/csrncd_decomp['Kb_H']*csrncd_decomp['gammac_H']))/csrncd_decomp['HIGH']
    csrncd_decomp.loc[(csrncd_decomp['L_est']<0)&(csrncd_decomp['Kb_L']>0)&(csrncd_decomp['Sb*_L']==csrncd_decomp['Kb_L']),'pder_L']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_L'])*(1+1/csrncd_decomp['Kb_L']*csrncd_decomp['gammac_L']))/csrncd_decomp['LOW']

    #case 4
    csrncd_decomp.loc[(csrncd_decomp['M_est']<0)&(csrncd_decomp['Kb_M']>0)&(csrncd_decomp['Sb*_M']+csrncd_decomp['Kb_M']==0),'pder_M']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_M'])*(1-1/csrncd_decomp['Kb_M']*csrncd_decomp['gammac_M']))/csrncd_decomp['NORMAL']
    csrncd_decomp.loc[(csrncd_decomp['H_est']<0)&(csrncd_decomp['Kb_H']>0)&(csrncd_decomp['Sb*_H']+csrncd_decomp['Kb_H']==0),'pder_H']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_H'])*(1-1/csrncd_decomp['Kb_H']*csrncd_decomp['gammac_H']))/csrncd_decomp['HIGH']
    csrncd_decomp.loc[(csrncd_decomp['L_est']<0)&(csrncd_decomp['Kb_L']>0)&(csrncd_decomp['Sb*_L']+csrncd_decomp['Kb_L']==0),'pder_L']=((csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_L'])*(1-1/csrncd_decomp['Kb_L']*csrncd_decomp['gammac_L']))/csrncd_decomp['LOW']

    #case 5
    csrncd_decomp.loc[(csrncd_decomp['M_est']<0)&(csrncd_decomp['Kb_M']>0)&(abs(csrncd_decomp['Sb*_M'])!=abs(csrncd_decomp['Kb_M'])),'pder_M']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_M']+csrncd_decomp['gammac_M'])/csrncd_decomp['NORMAL']
    csrncd_decomp.loc[(csrncd_decomp['H_est']<0)&(csrncd_decomp['Kb_H']>0)&(abs(csrncd_decomp['Sb*_H'])!=abs(csrncd_decomp['Kb_H'])),'pder_H']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_H']+csrncd_decomp['gammac_H'])/csrncd_decomp['HIGH']
    csrncd_decomp.loc[(csrncd_decomp['L_est']<0)&(csrncd_decomp['Kb_L']>0)&(abs(csrncd_decomp['Sb*_L'])!=abs(csrncd_decomp['Kb_L'])),'pder_L']=(csrncd_decomp['WEIGHTED_SENSITIVITY']+csrncd_decomp['rhol_L']+csrncd_decomp['gammac_L'])/csrncd_decomp['LOW']

    #case 6
    csrncd_decomp.loc[(csrncd_decomp['M_est']<0)&(csrncd_decomp['Kb_M']==0),'pder_M']=0
    csrncd_decomp.loc[(csrncd_decomp['H_est']<0)&(csrncd_decomp['Kb_H']==0),'pder_H']=0
    csrncd_decomp.loc[(csrncd_decomp['L_est']<0)&(csrncd_decomp['Kb_L']==0),'pder_L']=0

    csrncd_decomp=csrncd_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]

    csrncd_decomp_rslt=CSRNC_delta.merge(csrncd_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'],how='left')

    return CSRNC_delta, CSRNC_delta_agg, csrncd, csrncd_decomp_rslt


# ##### CSRNC_Vega

# In[47]:


def CSRNC_Vega(Raw_Data):
    return CSRNC_vega, CSRNC_vega_agg, csrncv, csrncv_decomp_rslt


# ##### CSRNC_Curvature

# In[48]:


def CSRNC_Curvature(Raw_Data):
    return CSRNC_curvature, CSRNC_curvature_agg, csrncc, csrncc_decomp_rslt


# #### CSR (ctp)

# ##### CSRC_Delta

# In[71]:


def CSRC_Delta(Raw_Data):
    return CSRC_delta, CSRC_delta_agg, csrcd, csrcd_decomp_rslt


# ##### CSRC_Vega

# In[72]:


def CSRC_Vega(Raw_Data):
    return CSRC_vega, CSRC_vega_agg, csrcv, csrcv_decomp_rslt


# ##### CSRC_Curvature

# In[73]:


def CSRC_Curvature(Raw_Data):
    return CSRC_curvature, CSRC_curvature_agg, csrcc, csrcc_decomp_rslt


# #### EQ

# ##### EQ_Delta

# In[ ]:


def EQ_Delta(Raw_Data):

    # get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')

    EQ_Weights = getParam('Equity_Weights')
    EQ_Rho = getParam('Equity_Rho')
    EQ_Rho_Diff = getParam('Equity_Rho_Diff')
    EQ_Gamma = getParam('Equity_Gamma')
    EQ_Big_RW = getParam('Equity_Big_RW')
    EQ_Small_RW = getParam('Equity_Small_RW')

    # get EQ Delta
    EQ_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='EQ')]
    EQ_RawData['ISSUER'] = EQ_RawData['RISK_FACTOR_ID']

    EQ_Position = EQ_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','ISSUER'
                              ,'RISK_FACTOR_TYPE','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]
    EQ_Position = EQ_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'
                                       ,'ISSUER','RISK_FACTOR_TYPE','SENSITIVITY_TYPE']
                                      ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()
    EQ_delta = EQ_Position[(EQ_Position['SENSITIVITY_TYPE']=='Delta')]

    EQ_delta['RISK_FACTOR_BUCKET']=EQ_delta['RISK_FACTOR_BUCKET'].astype(int)
    EQ_delta = EQ_delta.merge(EQ_Weights,on='RISK_FACTOR_BUCKET',how='left')
    EQ_delta.loc[EQ_delta['RISK_FACTOR_TYPE']=='Spot','RISKWEIGHT']=EQ_delta['Risk_Weight_Spot']
    EQ_delta.loc[EQ_delta['RISK_FACTOR_TYPE']=='Repo','RISKWEIGHT']=EQ_delta['Risk_Weight_Repo']
    EQ_delta['WEIGHTED_SENSITIVITY']=EQ_delta['SENSITIVITY_VAL_RPT_CURR_CNY']*EQ_delta['RISKWEIGHT']

    EQ_delta_kl=EQ_delta.rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_K'
                                   ,'RISK_FACTOR_TYPE':'RISK_FACTOR_TYPE_K'
                                   ,'ISSUER':'ISSUER_K'
                                   ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_K'},axis=1
                                 ).merge(EQ_delta[['RISK_FACTOR_ID','RISK_FACTOR_BUCKET'
                                                    ,'RISK_FACTOR_TYPE'
                                                    ,'ISSUER'
                                                    ,'WEIGHTED_SENSITIVITY']]
                                         .rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_L'
                                                  ,'RISK_FACTOR_TYPE':'RISK_FACTOR_TYPE_L'
                                                  ,'ISSUER':'ISSUER_L'
                                                  ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_L'},axis=1)
                                         ,on=['RISK_FACTOR_BUCKET'],how='left')

    EQ_delta_kl = EQ_delta_kl.merge(EQ_Rho,on=['RISK_FACTOR_BUCKET'],how='left')
    EQ_delta_kl.loc[(EQ_delta_kl['ISSUER_K']==EQ_delta_kl['ISSUER_L'])&(EQ_delta_kl['RISK_FACTOR_TYPE_K']!=EQ_delta_kl['RISK_FACTOR_TYPE_L']),'Rho_kl_M']=Equity_Rho_Diff
    EQ_delta_kl.loc[(EQ_delta_kl['ISSUER_K']!=EQ_delta_kl['ISSUER_L'])&(EQ_delta_kl['RISK_FACTOR_TYPE_K']!=EQ_delta_kl['RISK_FACTOR_TYPE_L']),'Rho_kl_M']=EQ_delta_kl['Rho_KL']*Equity_Rho_Diff
    EQ_delta_kl.loc[EQ_delta_kl['RISK_FACTOR_TYPE_K']==EQ_delta_kl['RISK_FACTOR_TYPE_L'],'Rho_kl_M']=EQ_delta_kl['Rho_KL']

    EQ_delta_kl['Rho_kl_H']=np.minimum(1,High_Multipler*EQ_delta_kl['Rho_kl_M'])
    EQ_delta_kl['Rho_kl_L']=np.maximum(Low_Multipler1*EQ_delta_kl['Rho_kl_M']-1,Low_Multipler2*EQ_delta_kl['Rho_kl_M'])

    EQ_delta_kl['rslt_kl_M']=EQ_delta_kl['WEIGHTED_SENSITIVITY_K']*EQ_delta_kl['WEIGHTED_SENSITIVITY_L']*EQ_delta_kl['Rho_kl_M']
    EQ_delta_kl['rslt_kl_H']=EQ_delta_kl['WEIGHTED_SENSITIVITY_K']*EQ_delta_kl['WEIGHTED_SENSITIVITY_L']*EQ_delta_kl['Rho_kl_H']
    EQ_delta_kl['rslt_kl_L']=EQ_delta_kl['WEIGHTED_SENSITIVITY_K']*EQ_delta_kl['WEIGHTED_SENSITIVITY_L']*EQ_delta_kl['Rho_kl_L']

    EQ_delta_kl.loc[(EQ_delta_kl.RISK_FACTOR_ID_K==EQ_delta_kl.RISK_FACTOR_ID_L)
                   &(EQ_delta_kl.RISK_FACTOR_TYPE_K==EQ_delta_kl.RISK_FACTOR_TYPE_L),'rhol_M']=0
    EQ_delta_kl.loc[(EQ_delta_kl.RISK_FACTOR_ID_K==EQ_delta_kl.RISK_FACTOR_ID_L)
                   &(EQ_delta_kl.RISK_FACTOR_TYPE_K==EQ_delta_kl.RISK_FACTOR_TYPE_L),'rhol_H']=0
    EQ_delta_kl.loc[(EQ_delta_kl.RISK_FACTOR_ID_K==EQ_delta_kl.RISK_FACTOR_ID_L)
                   &(EQ_delta_kl.RISK_FACTOR_TYPE_K==EQ_delta_kl.RISK_FACTOR_TYPE_L),'rhol_L']=0

    EQ_delta_kl.loc[(EQ_delta_kl.RISK_FACTOR_ID_K!=EQ_delta_kl.RISK_FACTOR_ID_L)
                   |(EQ_delta_kl.RISK_FACTOR_TYPE_K!=EQ_delta_kl.RISK_FACTOR_TYPE_L),'rhol_M']=EQ_delta_kl['WEIGHTED_SENSITIVITY_L']*EQ_delta_kl['Rho_kl_M']
    EQ_delta_kl.loc[(EQ_delta_kl.RISK_FACTOR_ID_K!=EQ_delta_kl.RISK_FACTOR_ID_L)
                   |(EQ_delta_kl.RISK_FACTOR_TYPE_K!=EQ_delta_kl.RISK_FACTOR_TYPE_L),'rhol_H']=EQ_delta_kl['WEIGHTED_SENSITIVITY_L']*EQ_delta_kl['Rho_kl_H']
    EQ_delta_kl.loc[(EQ_delta_kl.RISK_FACTOR_ID_K!=EQ_delta_kl.RISK_FACTOR_ID_L)
                   |(EQ_delta_kl.RISK_FACTOR_TYPE_K!=EQ_delta_kl.RISK_FACTOR_TYPE_L),'rhol_L']=EQ_delta_kl['WEIGHTED_SENSITIVITY_L']*EQ_delta_kl['Rho_kl_L']

    EQ_delta_agg=EQ_delta.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()

    EQ_delta_bc=EQ_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                                   ).merge(EQ_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c'
                                                                ,'WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                                           ,on =['RISK_FACTOR_CLASS'],how='left')

    EQ_delta_bc=EQ_delta_bc.merge(EQ_Gamma,on=['Bucket_b','Bucket_c'],how='left').rename({'Gamma_bc':'Gamma_bc_M'},axis=1)

    EQ_delta_bc['Gamma_bc_H']=np.minimum(1,High_Multipler*EQ_delta_bc['Gamma_bc_M'])
    EQ_delta_bc['Gamma_bc_L']=np.maximum(Low_Multipler1*EQ_delta_bc['Gamma_bc_M']-1,Low_Multipler2*EQ_delta_bc['Gamma_bc_M'])

    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']==EQ_delta_bc['Bucket_c'],'rslt_bc_M']=0
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']!=EQ_delta_bc['Bucket_c'],'rslt_bc_M']=EQ_delta_bc['WS_b']*EQ_delta_bc['WS_c']*EQ_delta_bc['Gamma_bc_M']
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']==EQ_delta_bc['Bucket_c'],'rslt_bc_H']=0
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']!=EQ_delta_bc['Bucket_c'],'rslt_bc_H']=EQ_delta_bc['WS_b']*EQ_delta_bc['WS_c']*EQ_delta_bc['Gamma_bc_H']
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']==EQ_delta_bc['Bucket_c'],'rslt_bc_L']=0
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']!=EQ_delta_bc['Bucket_c'],'rslt_bc_L']=EQ_delta_bc['WS_b']*EQ_delta_bc['WS_c']*EQ_delta_bc['Gamma_bc_L']

    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']==EQ_delta_bc['Bucket_c'],'gammac_M']=0
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']!=EQ_delta_bc['Bucket_c'],'gammac_M']=EQ_delta_bc['WS_c']*EQ_delta_bc['Gamma_bc_M']
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']==EQ_delta_bc['Bucket_c'],'gammac_H']=0
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']!=EQ_delta_bc['Bucket_c'],'gammac_H']=EQ_delta_bc['WS_c']*EQ_delta_bc['Gamma_bc_H']
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']==EQ_delta_bc['Bucket_c'],'gammac_L']=0
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']!=EQ_delta_bc['Bucket_c'],'gammac_L']=EQ_delta_bc['WS_c']*EQ_delta_bc['Gamma_bc_L']

    EQ_delta_agg=EQ_delta_agg.merge(EQ_delta_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']]
                                    ,on=['RISK_FACTOR_BUCKET'],how='left'
                                   ).groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']
                                             ,dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()

    EQ_delta_agg['Sb_H']=EQ_delta_agg['WEIGHTED_SENSITIVITY']
    EQ_delta_agg['Sb_L']=EQ_delta_agg['WEIGHTED_SENSITIVITY']
    EQ_delta_agg['Kb_M']=np.sqrt(EQ_delta_agg['rslt_kl_M'])
    EQ_delta_agg['Kb_H']=np.sqrt(EQ_delta_agg['rslt_kl_H'])
    EQ_delta_agg['Kb_L']=np.sqrt(EQ_delta_agg['rslt_kl_L'])

    EQ_delta_agg=EQ_delta_agg.rename({'WEIGHTED_SENSITIVITY':'Sb_M','rslt_kl_M':'Kb_M^2','rslt_kl_H':'Kb_H^2','rslt_kl_L':'Kb_L^2'},axis=1)

    EQ_delta_agg['Sb*_M']=np.maximum(np.minimum(EQ_delta_agg['Kb_M'],EQ_delta_agg['Sb_M']),-EQ_delta_agg['Kb_M'])
    EQ_delta_agg['Sb*_H']=np.maximum(np.minimum(EQ_delta_agg['Kb_H'],EQ_delta_agg['Sb_H']),-EQ_delta_agg['Kb_H'])
    EQ_delta_agg['Sb*_L']=np.maximum(np.minimum(EQ_delta_agg['Kb_L'],EQ_delta_agg['Sb_L']),-EQ_delta_agg['Kb_L'])

    EQ_delta_bc=EQ_delta_bc.merge(
        EQ_delta_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
        ,left_on=['Bucket_b'],right_on=['RISK_FACTOR_BUCKET'],how='left')
    EQ_delta_bc=EQ_delta_bc.merge(
        EQ_delta_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
        ,left_on=['Bucket_c'],right_on=['RISK_FACTOR_BUCKET'],how='left')
    EQ_delta_bc=EQ_delta_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)

    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']==EQ_delta_bc['Bucket_c'],'rslt_bc*_M']=0
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']!=EQ_delta_bc['Bucket_c'],'rslt_bc*_M']=EQ_delta_bc['Sb*_M']*EQ_delta_bc['Sc*_M']*EQ_delta_bc['Gamma_bc_M']
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']==EQ_delta_bc['Bucket_c'],'rslt_bc*_H']=0
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']!=EQ_delta_bc['Bucket_c'],'rslt_bc*_H']=EQ_delta_bc['Sb*_H']*EQ_delta_bc['Sc*_H']*EQ_delta_bc['Gamma_bc_H']
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']==EQ_delta_bc['Bucket_c'],'rslt_bc*_L']=0
    EQ_delta_bc.loc[EQ_delta_bc['Bucket_b']!=EQ_delta_bc['Bucket_c'],'rslt_bc*_L']=EQ_delta_bc['Sb*_L']*EQ_delta_bc['Sc*_L']*EQ_delta_bc['Gamma_bc_L']

    eqd = pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=['0'])

    eqd_M_est=sum(EQ_delta_agg['Kb_M^2'])+sum(EQ_delta_bc['rslt_bc_M'])
    eqd_M_1=np.sqrt(sum(EQ_delta_agg['Kb_M^2'])+sum(EQ_delta_bc['rslt_bc_M']))
    eqd_M_2=np.sqrt(sum(EQ_delta_agg['Kb_M^2'])+sum(EQ_delta_bc['rslt_bc*_M']))

    eqd_H_est=sum(EQ_delta_agg['Kb_H^2'])+sum(EQ_delta_bc['rslt_bc_H'])
    eqd_H_1=np.sqrt(sum(EQ_delta_agg['Kb_H^2'])+sum(EQ_delta_bc['rslt_bc_H']))
    eqd_H_2=np.sqrt(sum(EQ_delta_agg['Kb_H^2'])+sum(EQ_delta_bc['rslt_bc*_H']))

    eqd_L_est=sum(EQ_delta_agg['Kb_L^2'])+sum(EQ_delta_bc['rslt_bc_L'])
    eqd_L_1=np.sqrt(sum(EQ_delta_agg['Kb_L^2'])+sum(EQ_delta_bc['rslt_bc_L']))
    eqd_L_2=np.sqrt(sum(EQ_delta_agg['Kb_L^2'])+sum(EQ_delta_bc['rslt_bc*_L']))

    eqd['RISK_FACTOR_CLASS']='EQ'
    eqd['SENS_TYPE']='DELTA'
    eqd['NORMAL']=np.where(eqd_M_est>=0,eqd_M_1,eqd_M_2)
    eqd['HIGH']=np.where(eqd_H_est>=0,eqd_H_1,eqd_H_2)
    eqd['LOW']=np.where(eqd_L_est>=0,eqd_L_1,eqd_L_2)

    # Euler Decomposition
    eqd_1=EQ_delta[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','RISK_FACTOR_TYPE','WEIGHTED_SENSITIVITY']]
    eqd_2=EQ_delta_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_TYPE_K','RISK_FACTOR_BUCKET']
                              ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()
    eqd_3=EQ_delta_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                              ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    eqd_4=EQ_delta_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]

    eqd_decomp=eqd_1.merge(eqd_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_TYPE','RISK_FACTOR_BUCKET']
                           ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_TYPE_K','RISK_FACTOR_BUCKET'],how='left')\
    .merge(eqd_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(eqd_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(eqd,on=['RISK_FACTOR_CLASS'],how='left')

    eqd_decomp=eqd_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_TYPE_K','Bucket_b','SENS_TYPE'],axis=1)

    eqd_decomp['M_est']=eqd_M_est
    eqd_decomp['H_est']=eqd_H_est
    eqd_decomp['L_est']=eqd_L_est

    #1
    eqd_decomp.loc[(eqd_decomp['M_est']>=0)&(eqd_decomp['Kb_M']>0),'pderp_M']=(eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_M']+eqd_decomp['gammac_M'])/eqd_decomp['NORMAL']
    eqd_decomp.loc[(eqd_decomp['H_est']>=0)&(eqd_decomp['Kb_H']>0),'pderp_H']=(eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_H']+eqd_decomp['gammac_H'])/eqd_decomp['HIGH']
    eqd_decomp.loc[(eqd_decomp['L_est']>=0)&(eqd_decomp['Kb_L']>0),'pderp_L']=(eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_L']+eqd_decomp['gammac_L'])/eqd_decomp['LOW']

    eqd_decomp.loc[(eqd_decomp['M_est']>=0)&(eqd_decomp['Kb_M']>0),'pderm_M']=eqd_decomp['pderp_M']
    eqd_decomp.loc[(eqd_decomp['H_est']>=0)&(eqd_decomp['Kb_H']>0),'pderm_H']=eqd_decomp['pderp_H']
    eqd_decomp.loc[(eqd_decomp['L_est']>=0)&(eqd_decomp['Kb_L']>0),'pderm_L']=eqd_decomp['pderp_L']

    #2
    eqd_decomp.loc[(eqd_decomp['M_est']>=0)&(eqd_decomp['Kb_M']==0),'pderp_M']=eqd_decomp['gammac_M']/eqd_decomp['NORMAL']
    eqd_decomp.loc[(eqd_decomp['H_est']>=0)&(eqd_decomp['Kb_H']==0),'pderp_H']=eqd_decomp['gammac_H']/eqd_decomp['HIGH']
    eqd_decomp.loc[(eqd_decomp['L_est']>=0)&(eqd_decomp['Kb_L']==0),'pderp_L']=eqd_decomp['gammac_L']/eqd_decomp['LOW']

    eqd_decomp.loc[(eqd_decomp['M_est']>=0)&(eqd_decomp['Kb_M']==0),'pderm_M']=eqd_decomp['pderp_M']
    eqd_decomp.loc[(eqd_decomp['H_est']>=0)&(eqd_decomp['Kb_H']==0),'pderm_H']=eqd_decomp['pderp_H']
    eqd_decomp.loc[(eqd_decomp['L_est']>=0)&(eqd_decomp['Kb_L']==0),'pderm_L']=eqd_decomp['pderp_L']

    #3
    eqd_decomp.loc[(eqd_decomp['M_est']<0)&(eqd_decomp['Kb_M']>0)&(eqd_decomp['Sb*_M']==eqd_decomp['Kb_M'])
                    ,'pderp_M']=((eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_M'])*(1+1/eqd_decomp['Kb_M']*eqd_decomp['gammac_M']))/eqd_decomp['NORMAL']
    eqd_decomp.loc[(eqd_decomp['H_est']<0)&(eqd_decomp['Kb_H']>0)&(eqd_decomp['Sb*_H']==eqd_decomp['Kb_H'])
                    ,'pderp_H']=((eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_H'])*(1+1/eqd_decomp['Kb_H']*eqd_decomp['gammac_H']))/eqd_decomp['HIGH']
    eqd_decomp.loc[(eqd_decomp['L_est']<0)&(eqd_decomp['Kb_L']>0)&(eqd_decomp['Sb*_L']==eqd_decomp['Kb_L'])
                    ,'pderp_L']=((eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_L'])*(1+1/eqd_decomp['Kb_L']*eqd_decomp['gammac_L']))/eqd_decomp['LOW']

    eqd_decomp.loc[(eqd_decomp['M_est']<0)&(eqd_decomp['Kb_M']>0)&(eqd_decomp['Sb*_M']==eqd_decomp['Kb_M'])
                    ,'pderm_M']=eqd_decomp['pderp_M']
    eqd_decomp.loc[(eqd_decomp['H_est']<0)&(eqd_decomp['Kb_H']>0)&(eqd_decomp['Sb*_H']==eqd_decomp['Kb_H'])
                    ,'pderm_H']=eqd_decomp['pderp_H']
    eqd_decomp.loc[(eqd_decomp['L_est']<0)&(eqd_decomp['Kb_L']>0)&(eqd_decomp['Sb*_L']==eqd_decomp['Kb_L'])
                    ,'pderm_L']=eqd_decomp['pderp_L']

    #4
    eqd_decomp.loc[(eqd_decomp['M_est']<0)&(eqd_decomp['Kb_M']>0)&(eqd_decomp['Sb*_M']+eqd_decomp['Kb_M']==0)
                    ,'pderp_M']=((eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_M'])*(1-1/eqd_decomp['Kb_M']*eqd_decomp['gammac_M']))/eqd_decomp['NORMAL']
    eqd_decomp.loc[(eqd_decomp['H_est']<0)&(eqd_decomp['Kb_H']>0)&(eqd_decomp['Sb*_H']+eqd_decomp['Kb_H']==0)
                    ,'pderp_H']=((eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_H'])*(1-1/eqd_decomp['Kb_H']*eqd_decomp['gammac_H']))/eqd_decomp['HIGH']
    eqd_decomp.loc[(eqd_decomp['L_est']<0)&(eqd_decomp['Kb_L']>0)&(eqd_decomp['Sb*_L']+eqd_decomp['Kb_L']==0)
                    ,'pderp_L']=((eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_L'])*(1-1/eqd_decomp['Kb_L']*eqd_decomp['gammac_L']))/eqd_decomp['LOW']

    eqd_decomp.loc[(eqd_decomp['M_est']<0)&(eqd_decomp['Kb_M']>0)&(eqd_decomp['Sb*_M']+eqd_decomp['Kb_M']==0)
                    ,'pderm_M']=eqd_decomp['pderp_M']
    eqd_decomp.loc[(eqd_decomp['H_est']<0)&(eqd_decomp['Kb_H']>0)&(eqd_decomp['Sb*_H']+eqd_decomp['Kb_H']==0)
                    ,'pderm_H']=eqd_decomp['pderp_H']
    eqd_decomp.loc[(eqd_decomp['L_est']<0)&(eqd_decomp['Kb_L']>0)&(eqd_decomp['Sb*_L']+eqd_decomp['Kb_L']==0)
                    ,'pderm_L']=eqd_decomp['pderp_L']

    #5
    eqd_decomp.loc[(eqd_decomp['M_est']<0)&(eqd_decomp['Kb_M']>0)&(abs(eqd_decomp['Sb*_M'])!=abs(eqd_decomp['Kb_M']))
                    ,'pderp_M']=(eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_M']+eqd_decomp['gammac_M'])/eqd_decomp['NORMAL']
    eqd_decomp.loc[(eqd_decomp['H_est']<0)&(eqd_decomp['Kb_H']>0)&(abs(eqd_decomp['Sb*_H'])!=abs(eqd_decomp['Kb_H']))
                    ,'pderp_H']=(eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_H']+eqd_decomp['gammac_H'])/eqd_decomp['HIGH']
    eqd_decomp.loc[(eqd_decomp['L_est']<0)&(eqd_decomp['Kb_L']>0)&(abs(eqd_decomp['Sb*_L'])!=abs(eqd_decomp['Kb_L']))
                    ,'pderp_L']=(eqd_decomp['WEIGHTED_SENSITIVITY']+eqd_decomp['rhol_L']+eqd_decomp['gammac_L'])/eqd_decomp['LOW']

    eqd_decomp.loc[(eqd_decomp['M_est']<0)&(eqd_decomp['Kb_M']>0)&(abs(eqd_decomp['Sb*_M'])!=abs(eqd_decomp['Kb_M']))
                    ,'pderm_M']=eqd_decomp['pderp_M']
    eqd_decomp.loc[(eqd_decomp['H_est']<0)&(eqd_decomp['Kb_H']>0)&(abs(eqd_decomp['Sb*_H'])!=abs(eqd_decomp['Kb_H']))
                    ,'pderm_H']=eqd_decomp['pderp_H']
    eqd_decomp.loc[(eqd_decomp['L_est']<0)&(eqd_decomp['Kb_L']>0)&(abs(eqd_decomp['Sb*_L'])!=abs(eqd_decomp['Kb_L']))
                    ,'pderm_L']=eqd_decomp['pderp_L']

    #6
    eqd_decomp.loc[(eqd_decomp['M_est']<0)&(eqd_decomp['Kb_M']==0),'pderp_M']=0
    eqd_decomp.loc[(eqd_decomp['H_est']<0)&(eqd_decomp['Kb_H']==0),'pderp_H']=0
    eqd_decomp.loc[(eqd_decomp['L_est']<0)&(eqd_decomp['Kb_L']==0),'pderp_L']=0

    eqd_decomp.loc[(eqd_decomp['M_est']<0)&(eqd_decomp['Kb_M']==0),'pderm_M']=0
    eqd_decomp.loc[(eqd_decomp['H_est']<0)&(eqd_decomp['Kb_H']==0),'pderm_H']=0
    eqd_decomp.loc[(eqd_decomp['L_est']<0)&(eqd_decomp['Kb_L']==0),'pderm_L']=0

    eqd_decomp=eqd_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_TYPE','RISK_FACTOR_BUCKET','pderp_M','pderp_H','pderp_L','pderm_M','pderm_H','pderm_L']]
    eqd_decomp_rslt=EQ_delta.merge(eqd_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_TYPE','RISK_FACTOR_BUCKET'],how='left')

    eqd_decomp_rslt.loc[(eqd_decomp_rslt.WEIGHTED_SENSITIVITY>=0),'pder_M']=eqd_decomp_rslt.pderp_M
    eqd_decomp_rslt.loc[(eqd_decomp_rslt.WEIGHTED_SENSITIVITY>=0),'pder_H']=eqd_decomp_rslt.pderp_H
    eqd_decomp_rslt.loc[(eqd_decomp_rslt.WEIGHTED_SENSITIVITY>=0),'pder_L']=eqd_decomp_rslt.pderp_L
    eqd_decomp_rslt.loc[(eqd_decomp_rslt.WEIGHTED_SENSITIVITY<0),'pder_M']=eqd_decomp_rslt.pderm_M
    eqd_decomp_rslt.loc[(eqd_decomp_rslt.WEIGHTED_SENSITIVITY<0),'pder_H']=eqd_decomp_rslt.pderm_H
    eqd_decomp_rslt.loc[(eqd_decomp_rslt.WEIGHTED_SENSITIVITY<0),'pder_L']=eqd_decomp_rslt.pderm_L

    eqd_decomp_rslt=eqd_decomp_rslt.fillna({'pder_M':0,'pder_H':0,'pder_L':0})

    return EQ_delta, EQ_delta_agg, eqd, eqd_decomp_rslt


# ##### EQ_Vega

# In[ ]:


def EQ_Vega(Raw_Data):
        
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')

    EQ_Weights = getParam('Equity_Weights')
    EQ_Rho = getParam('Equity_Rho')
    EQ_Rho_Diff = getParam('Equity_Rho_Diff')
    EQ_Gamma = getParam('Equity_Gamma')
    EQ_Big_RW = getParam('Equity_Big_RW')
    EQ_Small_RW = getParam('Equity_Small_RW')

    EQ_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='EQ')]
    EQ_RawData['ISSUER'] = EQ_RawData['RISK_FACTOR_ID']

    EQ_Position = EQ_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','ISSUER'
                              ,'RISK_FACTOR_TYPE','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]
    EQ_Position = EQ_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'
                                       ,'ISSUER','RISK_FACTOR_TYPE','SENSITIVITY_TYPE']
                                      ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    EQ_vega = EQ_Position[(EQ_Position['SENSITIVITY_TYPE']=='Vega')]

    EQ_vega.loc[(EQ_vega['RISK_FACTOR_BUCKET']<=8) | (EQ_vega['RISK_FACTOR_BUCKET']>=12), 'RISKWEIGHT']=EQ_Big_RW
    EQ_vega.loc[(EQ_vega['RISK_FACTOR_BUCKET']>=9) & (EQ_vega['RISK_FACTOR_BUCKET']<=11), 'RISKWEIGHT']=EQ_Small_RW

    EQ_vega = EQ_vega.assign(WEIGHTED_SENSITIVITY=EQ_vega['SENSITIVITY_VAL_RPT_CURR_CNY'] * EQ_vega['RISKWEIGHT'])

    EQ_vega_kl = EQ_vega.rename(
            {'RISK_FACTOR_ID':'RISK_FACTOR_ID_K'
             ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_K'  
             ,'ISSUER':'ISSUER_K'
             ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_K'},axis=1
        ).merge(EQ_vega[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','ISSUER','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
                .rename({'RISK_FACTOR_ID':'RISK_FACTOR_ID_L'
                         ,'RISK_FACTOR_VERTEX_1':'RISK_FACTOR_VERTEX_1_L'
                         ,'ISSUER':'ISSUER_L'
                         ,'WEIGHTED_SENSITIVITY':'WEIGHTED_SENSITIVITY_L'},axis=1)
                ,on=['RISK_FACTOR_BUCKET'],how='left')

    EQ_vega_kl = EQ_vega_kl.merge(EQ_Rho,on=['RISK_FACTOR_BUCKET'],how='left')

    EQ_vega_kl['Rho_kl_opt_mat_M'] = np.exp(
            -0.01*abs(
                EQ_vega_kl['RISK_FACTOR_VERTEX_1_K']-EQ_vega_kl['RISK_FACTOR_VERTEX_1_L']
            )/np.minimum(EQ_vega_kl['RISK_FACTOR_VERTEX_1_K'],EQ_vega_kl['RISK_FACTOR_VERTEX_1_L']))

    EQ_vega_kl['Rho_kl_M']=np.minimum((EQ_vega_kl['Rho_kl_opt_mat_M']*EQ_vega_kl['Rho_KL']),1)
    EQ_vega_kl['Rho_kl_H']=np.minimum(1,High_Multipler*EQ_vega_kl['Rho_kl_M'])
    EQ_vega_kl['Rho_kl_L']=np.maximum(Low_Multipler1*EQ_vega_kl['Rho_kl_M']-1,Low_Multipler2*EQ_vega_kl['Rho_kl_M'])

    EQ_vega_kl['rslt_kl_M']=EQ_vega_kl['Rho_kl_M']*EQ_vega_kl['WEIGHTED_SENSITIVITY_K']*EQ_vega_kl['WEIGHTED_SENSITIVITY_L']
    EQ_vega_kl['rslt_kl_H']=EQ_vega_kl['Rho_kl_H']*EQ_vega_kl['WEIGHTED_SENSITIVITY_K']*EQ_vega_kl['WEIGHTED_SENSITIVITY_L']
    EQ_vega_kl['rslt_kl_L']=EQ_vega_kl['Rho_kl_L']*EQ_vega_kl['WEIGHTED_SENSITIVITY_K']*EQ_vega_kl['WEIGHTED_SENSITIVITY_L']

    EQ_vega_kl.loc[(EQ_vega_kl.RISK_FACTOR_ID_K==EQ_vega_kl.RISK_FACTOR_ID_L)
                     &(EQ_vega_kl.RISK_FACTOR_VERTEX_1_K==EQ_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=0
    EQ_vega_kl.loc[(EQ_vega_kl.RISK_FACTOR_ID_K==EQ_vega_kl.RISK_FACTOR_ID_L)
                     &(EQ_vega_kl.RISK_FACTOR_VERTEX_1_K==EQ_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=0
    EQ_vega_kl.loc[(EQ_vega_kl.RISK_FACTOR_ID_K==EQ_vega_kl.RISK_FACTOR_ID_L)
                     &(EQ_vega_kl.RISK_FACTOR_VERTEX_1_K==EQ_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=0

    EQ_vega_kl.loc[(EQ_vega_kl.RISK_FACTOR_ID_K!=EQ_vega_kl.RISK_FACTOR_ID_L)
                     |(EQ_vega_kl.RISK_FACTOR_VERTEX_1_K!=EQ_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_M']=EQ_vega_kl['WEIGHTED_SENSITIVITY_L']*EQ_vega_kl['Rho_kl_M']
    EQ_vega_kl.loc[(EQ_vega_kl.RISK_FACTOR_ID_K!=EQ_vega_kl.RISK_FACTOR_ID_L)
                     |(EQ_vega_kl.RISK_FACTOR_VERTEX_1_K!=EQ_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_H']=EQ_vega_kl['WEIGHTED_SENSITIVITY_L']*EQ_vega_kl['Rho_kl_H']
    EQ_vega_kl.loc[(EQ_vega_kl.RISK_FACTOR_ID_K!=EQ_vega_kl.RISK_FACTOR_ID_L)
                     |(EQ_vega_kl.RISK_FACTOR_VERTEX_1_K!=EQ_vega_kl.RISK_FACTOR_VERTEX_1_L),'rhol_L']=EQ_vega_kl['WEIGHTED_SENSITIVITY_L']*EQ_vega_kl['Rho_kl_L']

    EQ_vega_agg = EQ_vega.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()

    EQ_vega_bc = EQ_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                   ).merge(EQ_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c','WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                           ,on=['RISK_FACTOR_CLASS'],how='left')

    EQ_vega_bc = EQ_vega_bc.merge(EQ_Gamma,on=['Bucket_b','Bucket_c'],how='left').rename({'Gamma_bc':'Gamma_bc_M'},axis=1)
    EQ_vega_bc['Gamma_bc_H'] = np.minimum(1, EQ_vega_bc['Gamma_bc_M']*High_Multipler)
    EQ_vega_bc['Gamma_bc_L'] = np.maximum((Low_Multipler1*EQ_vega_bc['Gamma_bc_M']-1),(Low_Multipler2*EQ_vega_bc['Gamma_bc_M']))

    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_M']==1,'rslt_bc_M']=0
    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_M']!=1,'rslt_bc_M']=EQ_vega_bc.WS_b*EQ_vega_bc.WS_c*EQ_vega_bc.Gamma_bc_M
    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_H']==1,'rslt_bc_H']=0
    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_H']!=1,'rslt_bc_H']=EQ_vega_bc.WS_b*EQ_vega_bc.WS_c*EQ_vega_bc.Gamma_bc_H
    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_L']==1,'rslt_bc_L']=0
    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_L']!=1,'rslt_bc_L']=EQ_vega_bc.WS_b*EQ_vega_bc.WS_c*EQ_vega_bc.Gamma_bc_L

    EQ_vega_bc.loc[EQ_vega_bc['Bucket_b']==EQ_vega_bc['Bucket_c'],'gammac_M']=0
    EQ_vega_bc.loc[EQ_vega_bc['Bucket_b']!=EQ_vega_bc['Bucket_c'],'gammac_M']=EQ_vega_bc.WS_c*EQ_vega_bc.Gamma_bc_M
    EQ_vega_bc.loc[EQ_vega_bc['Bucket_b']==EQ_vega_bc['Bucket_c'],'gammac_H']=0
    EQ_vega_bc.loc[EQ_vega_bc['Bucket_b']!=EQ_vega_bc['Bucket_c'],'gammac_H']=EQ_vega_bc.WS_c*EQ_vega_bc.Gamma_bc_H
    EQ_vega_bc.loc[EQ_vega_bc['Bucket_b']==EQ_vega_bc['Bucket_c'],'gammac_L']=0
    EQ_vega_bc.loc[EQ_vega_bc['Bucket_b']!=EQ_vega_bc['Bucket_c'],'gammac_L']=EQ_vega_bc.WS_c*EQ_vega_bc.Gamma_bc_L

    EQ_vega_agg=EQ_vega_agg.merge(
        EQ_vega_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']],on=['RISK_FACTOR_BUCKET'],how='left'
    ).groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']
              ,dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()

    EQ_vega_agg['Sb_H']=EQ_vega_agg['WEIGHTED_SENSITIVITY']
    EQ_vega_agg['Sb_L']=EQ_vega_agg['WEIGHTED_SENSITIVITY']
    EQ_vega_agg['Kb_M']=np.sqrt(EQ_vega_agg['rslt_kl_M'])
    EQ_vega_agg['Kb_H']=np.sqrt(EQ_vega_agg['rslt_kl_H'])
    EQ_vega_agg['Kb_L']=np.sqrt(EQ_vega_agg['rslt_kl_L'])

    EQ_vega_agg = EQ_vega_agg.rename({'WEIGHTED_SENSITIVITY':'Sb_M','rslt_kl_M':'Kb_M^2','rslt_kl_H':'Kb_H^2','rslt_kl_L':'Kb_L^2'},axis=1)

    EQ_vega_agg['Sb*_M']=np.maximum(np.minimum(EQ_vega_agg['Kb_M'],EQ_vega_agg['Sb_M']),-EQ_vega_agg['Kb_M'])
    EQ_vega_agg['Sb*_H']=np.maximum(np.minimum(EQ_vega_agg['Kb_H'],EQ_vega_agg['Sb_H']),-EQ_vega_agg['Kb_H'])
    EQ_vega_agg['Sb*_L']=np.maximum(np.minimum(EQ_vega_agg['Kb_L'],EQ_vega_agg['Sb_L']),-EQ_vega_agg['Kb_L'])

    EQ_vega_bc=EQ_vega_bc.merge(
        EQ_vega_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
        ,left_on=['Bucket_b'],right_on=['RISK_FACTOR_BUCKET'],how='left')
    EQ_vega_bc=EQ_vega_bc.merge(
        EQ_vega_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
        ,left_on=['Bucket_c'],right_on=['RISK_FACTOR_BUCKET'],how='left')
    EQ_vega_bc=EQ_vega_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)

    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_M']==1,'rslt_bc*_M']=0
    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_M']!=1,'rslt_bc*_M']=EQ_vega_bc['Sb*_M']*EQ_vega_bc['Sc*_M']*EQ_vega_bc['Gamma_bc_M']
    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_H']==1,'rslt_bc*_H']=0
    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_H']!=1,'rslt_bc*_H']=EQ_vega_bc['Sb*_H']*EQ_vega_bc['Sc*_H']*EQ_vega_bc['Gamma_bc_H']
    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_L']==1,'rslt_bc*_L']=0
    EQ_vega_bc.loc[EQ_vega_bc['Gamma_bc_L']!=1,'rslt_bc*_L']=EQ_vega_bc['Sb*_L']*EQ_vega_bc['Sc*_L']*EQ_vega_bc['Gamma_bc_L']

    eqv=pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])

    eqv_M_est=sum(EQ_vega_agg['Kb_M^2'])+sum(EQ_vega_bc['rslt_bc_M'])
    eqv_M_1=np.sqrt(sum(EQ_vega_agg['Kb_M^2'])+sum(EQ_vega_bc['rslt_bc_M']))
    eqv_M_2=np.sqrt(sum(EQ_vega_agg['Kb_M^2'])+sum(EQ_vega_bc['rslt_bc*_M']))

    eqv_H_est=sum(EQ_vega_agg['Kb_H^2'])+sum(EQ_vega_bc['rslt_bc_H'])
    eqv_H_1=np.sqrt(sum(EQ_vega_agg['Kb_H^2'])+sum(EQ_vega_bc['rslt_bc_H']))
    eqv_H_2=np.sqrt(sum(EQ_vega_agg['Kb_H^2'])+sum(EQ_vega_bc['rslt_bc*_H']))

    eqv_L_est=sum(EQ_vega_agg['Kb_L^2'])+sum(EQ_vega_bc['rslt_bc_L'])
    eqv_L_1=np.sqrt(sum(EQ_vega_agg['Kb_L^2'])+sum(EQ_vega_bc['rslt_bc_L']))
    eqv_L_2=np.sqrt(sum(EQ_vega_agg['Kb_L^2'])+sum(EQ_vega_bc['rslt_bc*_L']))

    eqv['RISK_FACTOR_CLASS']='EQ'
    eqv['SENS_TYPE']='VEGA'
    eqv['NORMAL']=np.where(eqv_M_est>=0,eqv_M_1,eqv_M_2)
    eqv['HIGH']=np.where(eqv_H_est>=0,eqv_H_1,eqv_H_2)
    eqv['LOW']=np.where(eqv_L_est>=0,eqv_L_1,eqv_L_2)

    # Euler Decomposition
    eqv_1=EQ_vega[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
    eqv_2=EQ_vega_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                             ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()
    eqv_3=EQ_vega_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                             ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    eqv_4=EQ_vega_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]

    eqv_decomp=eqv_1.merge(eqv_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET']
                           ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                           ,how='left')\
    .merge(eqv_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(eqv_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(eqv,on=['RISK_FACTOR_CLASS'],how='left')

    eqv_decomp=eqv_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','Bucket_b','SENS_TYPE'],axis=1)

    eqv_decomp['M_est']=eqv_M_est
    eqv_decomp['H_est']=eqv_H_est
    eqv_decomp['L_est']=eqv_L_est

    #case 1
    eqv_decomp.loc[(eqv_decomp['M_est']>=0)&(eqv_decomp['Kb_M']>0),'pder_M']=(eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_M']+eqv_decomp['gammac_M'])/eqv_decomp['NORMAL']
    eqv_decomp.loc[(eqv_decomp['H_est']>=0)&(eqv_decomp['Kb_H']>0),'pder_H']=(eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_H']+eqv_decomp['gammac_H'])/eqv_decomp['HIGH']
    eqv_decomp.loc[(eqv_decomp['L_est']>=0)&(eqv_decomp['Kb_L']>0),'pder_L']=(eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_L']+eqv_decomp['gammac_L'])/eqv_decomp['LOW']

    #case 2
    eqv_decomp.loc[(eqv_decomp['M_est']>=0)&(eqv_decomp['Kb_M']==0),'pder_M']=eqv_decomp['gammac_M']/eqv_decomp['NORMAL']
    eqv_decomp.loc[(eqv_decomp['H_est']>=0)&(eqv_decomp['Kb_H']==0),'pder_H']=eqv_decomp['gammac_H']/eqv_decomp['HIGH']
    eqv_decomp.loc[(eqv_decomp['L_est']>=0)&(eqv_decomp['Kb_L']==0),'pder_L']=eqv_decomp['gammac_L']/eqv_decomp['LOW']

    #case 3
    eqv_decomp.loc[(eqv_decomp['M_est']<0)&(eqv_decomp['Kb_M']>0)&(eqv_decomp['Sb*_M']==eqv_decomp['Kb_M']),'pder_M']=((eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_M'])*(1+1/eqv_decomp['Kb_M']*eqv_decomp['gammac_M']))/eqv_decomp['NORMAL']
    eqv_decomp.loc[(eqv_decomp['H_est']<0)&(eqv_decomp['Kb_H']>0)&(eqv_decomp['Sb*_H']==eqv_decomp['Kb_H']),'pder_H']=((eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_H'])*(1+1/eqv_decomp['Kb_H']*eqv_decomp['gammac_H']))/eqv_decomp['HIGH']
    eqv_decomp.loc[(eqv_decomp['L_est']<0)&(eqv_decomp['Kb_L']>0)&(eqv_decomp['Sb*_L']==eqv_decomp['Kb_L']),'pder_L']=((eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_L'])*(1+1/eqv_decomp['Kb_L']*eqv_decomp['gammac_L']))/eqv_decomp['LOW']

    #case 4
    eqv_decomp.loc[(eqv_decomp['M_est']<0)&(eqv_decomp['Kb_M']>0)&(eqv_decomp['Sb*_M']+eqv_decomp['Kb_M']==0),'pder_M']=((eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_M'])*(1-1/eqv_decomp['Kb_M']*eqv_decomp['gammac_M']))/eqv_decomp['NORMAL']
    eqv_decomp.loc[(eqv_decomp['H_est']<0)&(eqv_decomp['Kb_H']>0)&(eqv_decomp['Sb*_H']+eqv_decomp['Kb_H']==0),'pder_H']=((eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_H'])*(1-1/eqv_decomp['Kb_H']*eqv_decomp['gammac_H']))/eqv_decomp['HIGH']
    eqv_decomp.loc[(eqv_decomp['L_est']<0)&(eqv_decomp['Kb_L']>0)&(eqv_decomp['Sb*_L']+eqv_decomp['Kb_L']==0),'pder_L']=((eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_L'])*(1-1/eqv_decomp['Kb_L']*eqv_decomp['gammac_L']))/eqv_decomp['LOW']

    #case 5
    eqv_decomp.loc[(eqv_decomp['M_est']<0)&(eqv_decomp['Kb_M']>0)&(abs(eqv_decomp['Sb*_M'])!=abs(eqv_decomp['Kb_M'])),'pder_M']=(eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_M']+eqv_decomp['gammac_M'])/eqv_decomp['NORMAL']
    eqv_decomp.loc[(eqv_decomp['H_est']<0)&(eqv_decomp['Kb_H']>0)&(abs(eqv_decomp['Sb*_H'])!=abs(eqv_decomp['Kb_H'])),'pder_H']=(eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_H']+eqv_decomp['gammac_H'])/eqv_decomp['HIGH']
    eqv_decomp.loc[(eqv_decomp['L_est']<0)&(eqv_decomp['Kb_L']>0)&(abs(eqv_decomp['Sb*_L'])!=abs(eqv_decomp['Kb_L'])),'pder_L']=(eqv_decomp['WEIGHTED_SENSITIVITY']+eqv_decomp['rhol_L']+eqv_decomp['gammac_L'])/eqv_decomp['LOW']

    #case 6
    eqv_decomp.loc[(eqv_decomp['M_est']<0)&(eqv_decomp['Kb_M']==0),'pder_M']=0
    eqv_decomp.loc[(eqv_decomp['H_est']<0)&(eqv_decomp['Kb_H']==0),'pder_H']=0
    eqv_decomp.loc[(eqv_decomp['L_est']<0)&(eqv_decomp['Kb_L']==0),'pder_L']=0

    eqv_decomp=eqv_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]

    eqv_decomp_rslt=EQ_vega.merge(eqv_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'],how='left')

    eqv_decomp_rslt=eqv_decomp_rslt.fillna({'pder_M':0,'pder_H':0,'pder_L':0})
    
    return EQ_vega, EQ_vega_agg, eqv, eqv_decomp_rslt


# ##### EQ_Curvature

# In[ ]:


def EQ_Curvature(Raw_Data):

    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')

    EQ_Weights = getParam('Equity_Weights')
    EQ_Rho = getParam('Equity_Rho')
    EQ_Rho_Diff = getParam('Equity_Rho_Diff')
    EQ_Gamma = getParam('Equity_Gamma')
    EQ_Big_RW = getParam('Equity_Big_RW')
    EQ_Small_RW = getParam('Equity_Small_RW')

    EQ_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='EQ')]
    EQ_RawData['ISSUER'] = EQ_RawData['RISK_FACTOR_ID']

    EQ_Position = EQ_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','ISSUER'
                              ,'RISK_FACTOR_TYPE','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]
    EQ_Position = EQ_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'
                                       ,'ISSUER','RISK_FACTOR_TYPE','SENSITIVITY_TYPE']
                                      ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    EQ_curvature = EQ_Position.query('SENSITIVITY_TYPE=="Curvature Up"|SENSITIVITY_TYPE=="Curvature Down"')

    EQ_curvature = EQ_curvature.groupby(['RISK_FACTOR_ID','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE']
                                        ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    EQ_curvature = EQ_curvature.assign(max_0_square=np.square(np.maximum(EQ_curvature['SENSITIVITY_VAL_RPT_CURR_CNY'],0)))
    EQ_curvature = EQ_curvature.assign(WEIGHTED_SENSITIVITY=EQ_curvature['SENSITIVITY_VAL_RPT_CURR_CNY'])

    EQ_curvature_agg = EQ_curvature.groupby(
        ['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],dropna=False
    ).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum','max_0_square':'sum'}).reset_index()

    EQ_curvature_agg['max_0_k']=np.sqrt(EQ_curvature_agg['max_0_square'])

    EQ_curvature_agg=EQ_curvature_agg.pivot(index=('RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET')
                                            ,columns='SENSITIVITY_TYPE')

    EQ_curvature_agg.columns=['/'.join(i) for i in EQ_curvature_agg.columns]
    EQ_curvature_agg=EQ_curvature_agg.reset_index()

    EQ_curvature_agg['Kb+_M']=np.sqrt(np.maximum(0,(EQ_curvature_agg['max_0_square/Curvature Up'])))
    EQ_curvature_agg['Kb-_M']=np.sqrt(np.maximum(0,(EQ_curvature_agg['max_0_square/Curvature Down'])))
    EQ_curvature_agg['Kb_M']=np.maximum(EQ_curvature_agg['Kb+_M'],EQ_curvature_agg['Kb-_M'])
    EQ_curvature_agg['Kb_M^2']=np.square(EQ_curvature_agg['Kb_M'])
    EQ_curvature_agg['Sb_M']=np.select([(EQ_curvature_agg['Kb_M'] == EQ_curvature_agg['Kb+_M']),
                                          (EQ_curvature_agg['Kb_M'] != EQ_curvature_agg['Kb+_M'])],
                                         [(EQ_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (EQ_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    EQ_curvature_agg['Kb+_H']=np.sqrt(np.maximum(0,(EQ_curvature_agg['max_0_square/Curvature Up'])))
    EQ_curvature_agg['Kb-_H']=np.sqrt(np.maximum(0,(EQ_curvature_agg['max_0_square/Curvature Down'])))
    EQ_curvature_agg['Kb_H']=np.maximum(EQ_curvature_agg['Kb+_H'],EQ_curvature_agg['Kb-_H'])
    EQ_curvature_agg['Kb_H^2']=np.square(EQ_curvature_agg['Kb_H'])
    EQ_curvature_agg['Sb_H']=np.select([(EQ_curvature_agg['Kb_H'] == EQ_curvature_agg['Kb+_H']),
                                          (EQ_curvature_agg['Kb_H'] != EQ_curvature_agg['Kb+_H'])],
                                         [(EQ_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (EQ_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    EQ_curvature_agg['Kb+_L']=np.sqrt(np.maximum(0,(EQ_curvature_agg['max_0_square/Curvature Up'])))
    EQ_curvature_agg['Kb-_L']=np.sqrt(np.maximum(0,(EQ_curvature_agg['max_0_square/Curvature Down'])))
    EQ_curvature_agg['Kb_L']=np.maximum(EQ_curvature_agg['Kb+_L'],EQ_curvature_agg['Kb-_L'])
    EQ_curvature_agg['Kb_L^2']=np.square(EQ_curvature_agg['Kb_L'])
    EQ_curvature_agg['Sb_L']=np.select([(EQ_curvature_agg['Kb_L'] == EQ_curvature_agg['Kb+_L']),
                                          (EQ_curvature_agg['Kb_L'] != EQ_curvature_agg['Kb+_L'])],
                                         [(EQ_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (EQ_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    EQ_curvature_agg['max']=np.select([(EQ_curvature_agg['Kb_M'] == EQ_curvature_agg['Kb+_M']),
                                       (EQ_curvature_agg['Kb_M'] != EQ_curvature_agg['Kb+_M'])],
                                      [(EQ_curvature_agg['max_0_k/Curvature Up']),
                                       (EQ_curvature_agg['max_0_k/Curvature Down'])])
    EQ_curvature_agg['sign']=np.select([(EQ_curvature_agg['Kb_M'] == EQ_curvature_agg['Kb+_M']),
                                        (EQ_curvature_agg['Kb_M'] != EQ_curvature_agg['Kb+_M'])],
                                       ['Curvature Up','Curvature Down'])

    EQ_curvature_bc=EQ_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Sb_M']]
    EQ_curvature_bc=EQ_curvature_bc.rename(
        {'Sb_M':'Sb','RISK_FACTOR_BUCKET':'Bucket_b'},axis=1
    ).merge(EQ_curvature_bc.rename(
        {'Sb_M':'Sc','RISK_FACTOR_BUCKET':'Bucket_c'},axis=1
    ),on=['RISK_FACTOR_CLASS'],how='left')

    EQ_curvature_bc.loc[(EQ_curvature_bc['Sb']<0) & (EQ_curvature_bc['Sc']<0),'Psi']=0
    EQ_curvature_bc.loc[(EQ_curvature_bc['Sb']>=0) | (EQ_curvature_bc['Sc']>=0),'Psi']=1

    EQ_curvature_bc=EQ_curvature_bc.merge(EQ_Gamma,on=['Bucket_b','Bucket_c'],how='left')
    EQ_curvature_bc['Gamma_bc_M']=np.square(EQ_curvature_bc['Gamma_bc'])
    EQ_curvature_bc['Gamma_bc_H']=np.square(np.minimum(1,EQ_curvature_bc['Gamma_bc']*High_Multipler))
    EQ_curvature_bc['Gamma_bc_L']=np.square(np.maximum((Low_Multipler1*EQ_curvature_bc['Gamma_bc']-1),(Low_Multipler2*EQ_curvature_bc['Gamma_bc'])))

    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b==EQ_curvature_bc.Bucket_c),'rslt_bc_M']=0
    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b!=EQ_curvature_bc.Bucket_c),'rslt_bc_M']=EQ_curvature_bc['Gamma_bc_M']*EQ_curvature_bc['Psi']*EQ_curvature_bc['Sb']*EQ_curvature_bc['Sc']
    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b==EQ_curvature_bc.Bucket_c),'rslt_bc_H']=0
    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b!=EQ_curvature_bc.Bucket_c),'rslt_bc_H']=EQ_curvature_bc['Gamma_bc_H']*EQ_curvature_bc['Psi']*EQ_curvature_bc['Sb']*EQ_curvature_bc['Sc']
    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b==EQ_curvature_bc.Bucket_c),'rslt_bc_L']=0
    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b!=EQ_curvature_bc.Bucket_c),'rslt_bc_L']=EQ_curvature_bc['Gamma_bc_L']*EQ_curvature_bc['Psi']*EQ_curvature_bc['Sb']*EQ_curvature_bc['Sc']

    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b==EQ_curvature_bc.Bucket_c),'gammac_M']=0
    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b!=EQ_curvature_bc.Bucket_c),'gammac_M']=EQ_curvature_bc['Gamma_bc_M']*EQ_curvature_bc['Psi']*EQ_curvature_bc['Sc']
    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b==EQ_curvature_bc.Bucket_c),'gammac_H']=0
    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b!=EQ_curvature_bc.Bucket_c),'gammac_H']=EQ_curvature_bc['Gamma_bc_H']*EQ_curvature_bc['Psi']*EQ_curvature_bc['Sc']
    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b==EQ_curvature_bc.Bucket_c),'gammac_L']=0
    EQ_curvature_bc.loc[(EQ_curvature_bc.Bucket_b!=EQ_curvature_bc.Bucket_c),'gammac_L']=EQ_curvature_bc['Gamma_bc_L']*EQ_curvature_bc['Psi']*EQ_curvature_bc['Sc']

    eqc_M_est=sum(EQ_curvature_agg['Kb_M^2'])+sum(EQ_curvature_bc['rslt_bc_M'])
    eqc_H_est=sum(EQ_curvature_agg['Kb_H^2'])+sum(EQ_curvature_bc['rslt_bc_H'])
    eqc_L_est=sum(EQ_curvature_agg['Kb_L^2'])+sum(EQ_curvature_bc['rslt_bc_L'])

    eqc_M = np.sqrt(np.maximum(0,sum(EQ_curvature_agg['Kb_M^2'])+sum(EQ_curvature_bc['rslt_bc_M'])))
    eqc_H = np.sqrt(np.maximum(0,sum(EQ_curvature_agg['Kb_H^2'])+sum(EQ_curvature_bc['rslt_bc_H'])))
    eqc_L = np.sqrt(np.maximum(0,sum(EQ_curvature_agg['Kb_L^2'])+sum(EQ_curvature_bc['rslt_bc_L'])))

    eqc=pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])

    eqc['RISK_FACTOR_CLASS']='EQ'
    eqc['SENS_TYPE']='CURVATURE'
    eqc['NORMAL']=eqc_M
    eqc['HIGH']=eqc_H
    eqc['LOW']=eqc_L

    eqc_1=EQ_curvature[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]
    eqc_3=EQ_curvature_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                                      ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    eqc_4=EQ_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','max','sign']]

    eqc_decomp=eqc_1.merge(eqc_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET']
                               ,right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(eqc_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(eqc,on=['RISK_FACTOR_CLASS'],how='left')

    eqc_decomp=eqc_decomp.drop(['Bucket_b','SENS_TYPE'],axis=1)

    eqc_decomp['M_est']=eqc_M_est
    eqc_decomp['H_est']=eqc_H_est
    eqc_decomp['L_est']=eqc_L_est

    eqc_decomp=eqc_decomp[(eqc_decomp.SENSITIVITY_TYPE==eqc_decomp.sign)]

    #case 1/2
    eqc_decomp.loc[(eqc_decomp['M_est']>=0),'pder_M']=(eqc_decomp['max']+eqc_decomp['gammac_M'])/eqc_decomp['NORMAL']
    eqc_decomp.loc[(eqc_decomp['H_est']>=0),'pder_H']=(eqc_decomp['max']+eqc_decomp['gammac_H'])/eqc_decomp['HIGH']
    eqc_decomp.loc[(eqc_decomp['L_est']>=0),'pder_L']=(eqc_decomp['max']+eqc_decomp['gammac_L'])/eqc_decomp['LOW']

    #case 3 
    eqc_decomp.loc[(eqc_decomp['M_est']<0),'pder_M']=0
    eqc_decomp.loc[(eqc_decomp['H_est']<0),'pder_H']=0
    eqc_decomp.loc[(eqc_decomp['L_est']<0),'pder_L']=0

    eqc_decomp=eqc_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','pder_M','pder_H','pder_L']]

    eqc_decomp_rslt=EQ_curvature.merge(eqc_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],how='right')

    
    return EQ_curvature, EQ_curvature_agg, eqc, eqc_decomp_rslt


# #### CMTY

# ##### CMTY_Delta

# In[51]:


def CMTY_Delta(Raw_Data):
    # get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    CMTY_Weights = getParam('CMTY_Weights')
    CMTY_Rho_Cty = getParam('CMTY_Rho_Cty')
    CMTY_Rho_Tenor = getParam('CMTY_Rho_Tenor')
    CMTY_Rho_Basis = getParam('CMTY_Rho_Basis')
    CMTY_Gamma = getParam('CMTY_Gamma')
    CMTY_LH = getParam('CMTY_LH')
    CMTY_vega_rw = getParam('CMTY_vega_rw')
    
    CMTY_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='CMTY')]
    CMTY_RawData['COMM_ASSET'] = CMTY_RawData['RISK_FACTOR_ID'].str.split('&',expand=True)[0]
    CMTY_RawData['COMM_LOCATION'] = CMTY_RawData['RISK_FACTOR_ID'].str.split('&',expand=True)[1]

    CMTY_Position = CMTY_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                  'RISK_FACTOR_BUCKET','COMM_ASSET',
                                  'COMM_LOCATION','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    CMTY_Position = CMTY_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                           'RISK_FACTOR_BUCKET','COMM_ASSET',
                                           'COMM_LOCATION','SENSITIVITY_TYPE']
                                          ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    CMTY_delta = CMTY_Position[(CMTY_Position['SENSITIVITY_TYPE']=='Delta')]
    #CMTY_delta['RISK_FACTOR_BUCKET']=CMTY_delta['RISK_FACTOR_BUCKET'].astype(int)
    CMTY_delta = CMTY_delta.merge(CMTY_Weights,on='RISK_FACTOR_BUCKET',how='left')
    CMTY_delta = CMTY_delta.rename({'Risk_Weight':'RISKWEIGHT'},axis=1)
    CMTY_delta['WEIGHTED_SENSITIVITY']=CMTY_delta['SENSITIVITY_VAL_RPT_CURR_CNY']*CMTY_delta['RISKWEIGHT']

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

    CMTY_delta_kl = CMTY_delta_kl.merge(CMTY_Rho_Cty,on='RISK_FACTOR_BUCKET',how='left')

    CMTY_delta_kl.loc[CMTY_delta_kl['COMM_ASSET_K']==CMTY_delta_kl['COMM_ASSET_L'],'Rho_Cty']=1
    CMTY_delta_kl.loc[CMTY_delta_kl['COMM_ASSET_K']!=CMTY_delta_kl['COMM_ASSET_L'],'Rho_Cty']=CMTY_delta_kl['Rho']
    CMTY_delta_kl.loc[CMTY_delta_kl['RISK_FACTOR_VERTEX_1_K']==CMTY_delta_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=1
    CMTY_delta_kl.loc[CMTY_delta_kl['RISK_FACTOR_VERTEX_1_K']!=CMTY_delta_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=CMTY_Rho_Tenor
    CMTY_delta_kl.loc[CMTY_delta_kl['RISK_FACTOR_ID_K']==CMTY_delta_kl['RISK_FACTOR_ID_L'],'Rho_Basis']=1
    CMTY_delta_kl.loc[CMTY_delta_kl['RISK_FACTOR_ID_K']!=CMTY_delta_kl['RISK_FACTOR_ID_L'],'Rho_Basis']=CMTY_Rho_Basis

    CMTY_delta_kl['Rho_kl_M']=CMTY_delta_kl['Rho_Cty']*CMTY_delta_kl['Rho_Tenor']*CMTY_delta_kl['Rho_Basis']
    CMTY_delta_kl['Rho_kl_H']=np.minimum(1,High_Multipler*CMTY_delta_kl['Rho_kl_M'])
    CMTY_delta_kl['Rho_kl_L']=np.maximum(Low_Multipler1*CMTY_delta_kl['Rho_kl_M']-1,Low_Multipler2*CMTY_delta_kl['Rho_kl_M'])

    CMTY_delta_kl['rslt_kl_M']=CMTY_delta_kl['WEIGHTED_SENSITIVITY_K']*CMTY_delta_kl['WEIGHTED_SENSITIVITY_L']*CMTY_delta_kl['Rho_kl_M']
    CMTY_delta_kl['rslt_kl_H']=CMTY_delta_kl['WEIGHTED_SENSITIVITY_K']*CMTY_delta_kl['WEIGHTED_SENSITIVITY_L']*CMTY_delta_kl['Rho_kl_H']
    CMTY_delta_kl['rslt_kl_L']=CMTY_delta_kl['WEIGHTED_SENSITIVITY_K']*CMTY_delta_kl['WEIGHTED_SENSITIVITY_L']*CMTY_delta_kl['Rho_kl_L']

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

    CMTY_delta_agg=CMTY_delta.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()

    CMTY_delta_bc=CMTY_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                                       ).merge(CMTY_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c'
                                                                      ,'WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                                               ,on =['RISK_FACTOR_CLASS'],how='left')

    CMTY_delta_bc=CMTY_delta_bc.merge(CMTY_Gamma,on=['Bucket_b','Bucket_c'],how='left').rename({'Gamma_bc':'Gamma_bc_M'},axis=1)
    CMTY_delta_bc['Gamma_bc_H']=np.minimum(1,High_Multipler*CMTY_delta_bc['Gamma_bc_M'])
    CMTY_delta_bc['Gamma_bc_L']=np.maximum(Low_Multipler1*CMTY_delta_bc['Gamma_bc_M']-1,Low_Multipler2*CMTY_delta_bc['Gamma_bc_M'])

    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc_M']=0
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc_M']=CMTY_delta_bc['WS_b']*CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_M']
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc_H']=0
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc_H']=CMTY_delta_bc['WS_b']*CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_H']
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc_L']=0
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc_L']=CMTY_delta_bc['WS_b']*CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_L']

    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'gammac_M']=0
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'gammac_M']=CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_M']
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'gammac_H']=0
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'gammac_H']=CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_H']
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'gammac_L']=0
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'gammac_L']=CMTY_delta_bc['WS_c']*CMTY_delta_bc['Gamma_bc_L']

    CMTY_delta_agg=CMTY_delta_agg.merge(CMTY_delta_kl[['RISK_FACTOR_BUCKET','rslt_kl_M','rslt_kl_H','rslt_kl_L']]
                                        ,on=['RISK_FACTOR_BUCKET'],how='left'
                                       ).groupby (['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY'],dropna=False).agg({'rslt_kl_M':'sum','rslt_kl_H':'sum','rslt_kl_L':'sum'}).reset_index()

    CMTY_delta_agg['Sb_H']=CMTY_delta_agg['WEIGHTED_SENSITIVITY']
    CMTY_delta_agg['Sb_L']=CMTY_delta_agg['WEIGHTED_SENSITIVITY']
    CMTY_delta_agg['Kb_M']=np.sqrt(CMTY_delta_agg['rslt_kl_M'])
    CMTY_delta_agg['Kb_H']=np.sqrt(CMTY_delta_agg['rslt_kl_H'])
    CMTY_delta_agg['Kb_L']=np.sqrt(CMTY_delta_agg['rslt_kl_L'])
    CMTY_delta_agg=CMTY_delta_agg.rename({'WEIGHTED_SENSITIVITY':'Sb_M','rslt_kl_M':'Kb_M^2','rslt_kl_H':'Kb_H^2','rslt_kl_L':'Kb_L^2'},axis=1)

    CMTY_delta_agg['Sb*_M']=np.maximum(np.minimum(CMTY_delta_agg['Kb_M'],CMTY_delta_agg['Sb_M']),-CMTY_delta_agg['Kb_M'])
    CMTY_delta_agg['Sb*_H']=np.maximum(np.minimum(CMTY_delta_agg['Kb_H'],CMTY_delta_agg['Sb_H']),-CMTY_delta_agg['Kb_H'])
    CMTY_delta_agg['Sb*_L']=np.maximum(np.minimum(CMTY_delta_agg['Kb_L'],CMTY_delta_agg['Sb_L']),-CMTY_delta_agg['Kb_L'])

    CMTY_delta_bc=CMTY_delta_bc.merge(
        CMTY_delta_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
        ,left_on=['Bucket_b'],right_on=['RISK_FACTOR_BUCKET'],how='left')

    CMTY_delta_bc=CMTY_delta_bc.merge(
        CMTY_delta_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
        ,left_on=['Bucket_c'],right_on=['RISK_FACTOR_BUCKET'],how='left')

    CMTY_delta_bc=CMTY_delta_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)

    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc*_M']=0
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc*_M']=CMTY_delta_bc['Sb*_M']*CMTY_delta_bc['Sc*_M']*CMTY_delta_bc['Gamma_bc_M']
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc*_H']=0
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc*_H']=CMTY_delta_bc['Sb*_H']*CMTY_delta_bc['Sc*_H']*CMTY_delta_bc['Gamma_bc_H']
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']==CMTY_delta_bc['Bucket_c'],'rslt_bc*_L']=0
    CMTY_delta_bc.loc[CMTY_delta_bc['Bucket_b']!=CMTY_delta_bc['Bucket_c'],'rslt_bc*_L']=CMTY_delta_bc['Sb*_L']*CMTY_delta_bc['Sc*_L']*CMTY_delta_bc['Gamma_bc_L']

    cmtyd = pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=['0'])

    cmtyd_M_est=sum(CMTY_delta_agg['Kb_M^2'])+sum(CMTY_delta_bc['rslt_bc_M'])
    cmtyd_M_1=np.sqrt(sum(CMTY_delta_agg['Kb_M^2'])+sum(CMTY_delta_bc['rslt_bc_M']))
    cmtyd_M_2=np.sqrt(sum(CMTY_delta_agg['Kb_M^2'])+sum(CMTY_delta_bc['rslt_bc*_M']))

    cmtyd_H_est=sum(CMTY_delta_agg['Kb_H^2'])+sum(CMTY_delta_bc['rslt_bc_H'])
    cmtyd_H_1=np.sqrt(sum(CMTY_delta_agg['Kb_H^2'])+sum(CMTY_delta_bc['rslt_bc_H']))
    cmtyd_H_2=np.sqrt(sum(CMTY_delta_agg['Kb_H^2'])+sum(CMTY_delta_bc['rslt_bc*_H']))

    cmtyd_L_est=sum(CMTY_delta_agg['Kb_L^2'])+sum(CMTY_delta_bc['rslt_bc_L'])
    cmtyd_L_1=np.sqrt(sum(CMTY_delta_agg['Kb_L^2'])+sum(CMTY_delta_bc['rslt_bc_L']))
    cmtyd_L_2=np.sqrt(sum(CMTY_delta_agg['Kb_L^2'])+sum(CMTY_delta_bc['rslt_bc*_L']))

    cmtyd['RISK_FACTOR_CLASS']='CMTY'
    cmtyd['SENS_TYPE']='DELTA'
    cmtyd['NORMAL']=np.where(cmtyd_M_est>=0,cmtyd_M_1,cmtyd_M_2)
    cmtyd['HIGH']=np.where(cmtyd_H_est>=0,cmtyd_H_1,cmtyd_H_2)
    cmtyd['LOW']=np.where(cmtyd_L_est>=0,cmtyd_L_1,cmtyd_L_2)

    cmtyd_1=CMTY_delta[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','COMM_LOCATION','WEIGHTED_SENSITIVITY']]
    cmtyd_2=CMTY_delta_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','COMM_LOCATION_K','RISK_FACTOR_BUCKET']
                          ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()
    cmtyd_3=CMTY_delta_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                          ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    cmtyd_4=CMTY_delta_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]
    
    cmtyd_decomp=cmtyd_1.merge(cmtyd_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','COMM_LOCATION','RISK_FACTOR_BUCKET']
                              ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','COMM_LOCATION_K','RISK_FACTOR_BUCKET'],how='left')\
    .merge(cmtyd_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(cmtyd_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(cmtyd,on=['RISK_FACTOR_CLASS'],how='left')

    cmtyd_decomp=cmtyd_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','COMM_LOCATION_K','Bucket_b','SENS_TYPE'],axis=1)

    cmtyd_decomp['M_est']=cmtyd_M_est
    cmtyd_decomp['H_est']=cmtyd_H_est
    cmtyd_decomp['L_est']=cmtyd_L_est

    #case 1
    cmtyd_decomp.loc[(cmtyd_decomp['M_est']>=0)&(cmtyd_decomp['Kb_M']>0),'pder_M']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_M']+cmtyd_decomp['gammac_M'])/cmtyd_decomp['NORMAL']
    cmtyd_decomp.loc[(cmtyd_decomp['H_est']>=0)&(cmtyd_decomp['Kb_H']>0),'pder_H']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_H']+cmtyd_decomp['gammac_H'])/cmtyd_decomp['HIGH']
    cmtyd_decomp.loc[(cmtyd_decomp['L_est']>=0)&(cmtyd_decomp['Kb_L']>0),'pder_L']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_L']+cmtyd_decomp['gammac_L'])/cmtyd_decomp['LOW']

    #case 2
    cmtyd_decomp.loc[(cmtyd_decomp['M_est']>=0)&(cmtyd_decomp['Kb_M']==0),'pder_M']=cmtyd_decomp['gammac_M']/cmtyd_decomp['NORMAL']
    cmtyd_decomp.loc[(cmtyd_decomp['H_est']>=0)&(cmtyd_decomp['Kb_H']==0),'pder_H']=cmtyd_decomp['gammac_H']/cmtyd_decomp['HIGH']
    cmtyd_decomp.loc[(cmtyd_decomp['L_est']>=0)&(cmtyd_decomp['Kb_L']==0),'pder_L']=cmtyd_decomp['gammac_L']/cmtyd_decomp['LOW']

    #case 3
    cmtyd_decomp.loc[(cmtyd_decomp['M_est']<0)&(cmtyd_decomp['Kb_M']>0)&(cmtyd_decomp['Sb*_M']==cmtyd_decomp['Kb_M']),'pder_M']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_M'])*(1+1/cmtyd_decomp['Kb_M']*cmtyd_decomp['gammac_M']))/cmtyd_decomp['NORMAL']
    cmtyd_decomp.loc[(cmtyd_decomp['H_est']<0)&(cmtyd_decomp['Kb_H']>0)&(cmtyd_decomp['Sb*_H']==cmtyd_decomp['Kb_H']),'pder_H']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_H'])*(1+1/cmtyd_decomp['Kb_H']*cmtyd_decomp['gammac_H']))/cmtyd_decomp['HIGH']
    cmtyd_decomp.loc[(cmtyd_decomp['L_est']<0)&(cmtyd_decomp['Kb_L']>0)&(cmtyd_decomp['Sb*_L']==cmtyd_decomp['Kb_L']),'pder_L']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_L'])*(1+1/cmtyd_decomp['Kb_L']*cmtyd_decomp['gammac_L']))/cmtyd_decomp['LOW']

    #case 4
    cmtyd_decomp.loc[(cmtyd_decomp['M_est']<0)&(cmtyd_decomp['Kb_M']>0)&(cmtyd_decomp['Sb*_M']+cmtyd_decomp['Kb_M']==0),'pder_M']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_M'])*(1-1/cmtyd_decomp['Kb_M']*cmtyd_decomp['gammac_M']))/cmtyd_decomp['NORMAL']
    cmtyd_decomp.loc[(cmtyd_decomp['H_est']<0)&(cmtyd_decomp['Kb_H']>0)&(cmtyd_decomp['Sb*_H']+cmtyd_decomp['Kb_H']==0),'pder_H']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_H'])*(1-1/cmtyd_decomp['Kb_H']*cmtyd_decomp['gammac_H']))/cmtyd_decomp['HIGH']
    cmtyd_decomp.loc[(cmtyd_decomp['L_est']<0)&(cmtyd_decomp['Kb_L']>0)&(cmtyd_decomp['Sb*_L']+cmtyd_decomp['Kb_L']==0),'pder_L']=((cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_L'])*(1-1/cmtyd_decomp['Kb_L']*cmtyd_decomp['gammac_L']))/cmtyd_decomp['LOW']

    #case 5
    cmtyd_decomp.loc[(cmtyd_decomp['M_est']<0)&(cmtyd_decomp['Kb_M']>0)&(abs(cmtyd_decomp['Sb*_M'])!=abs(cmtyd_decomp['Kb_M'])),'pder_M']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_M']+cmtyd_decomp['gammac_M'])/cmtyd_decomp['NORMAL']
    cmtyd_decomp.loc[(cmtyd_decomp['H_est']<0)&(cmtyd_decomp['Kb_H']>0)&(abs(cmtyd_decomp['Sb*_H'])!=abs(cmtyd_decomp['Kb_H'])),'pder_H']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_H']+cmtyd_decomp['gammac_H'])/cmtyd_decomp['HIGH']
    cmtyd_decomp.loc[(cmtyd_decomp['L_est']<0)&(cmtyd_decomp['Kb_L']>0)&(abs(cmtyd_decomp['Sb*_L'])!=abs(cmtyd_decomp['Kb_L'])),'pder_L']=(cmtyd_decomp['WEIGHTED_SENSITIVITY']+cmtyd_decomp['rhol_L']+cmtyd_decomp['gammac_L'])/cmtyd_decomp['LOW']

    #case 6
    cmtyd_decomp.loc[(cmtyd_decomp['M_est']<0)&(cmtyd_decomp['Kb_M']==0),'pder_M']=0
    cmtyd_decomp.loc[(cmtyd_decomp['H_est']<0)&(cmtyd_decomp['Kb_H']==0),'pder_H']=0
    cmtyd_decomp.loc[(cmtyd_decomp['L_est']<0)&(cmtyd_decomp['Kb_L']==0),'pder_L']=0

    cmtyd_decomp=cmtyd_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','COMM_LOCATION','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]

    cmtyd_decomp_rslt=CMTY_delta.merge(cmtyd_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','COMM_LOCATION','RISK_FACTOR_BUCKET'],how='left')

    return CMTY_delta, CMTY_delta_agg, cmtyd, cmtyd_decomp_rslt


# ##### CMTY_Vega

# In[54]:


def CMTY_Vega(Raw_Data):
    # get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    CMTY_Weights = getParam('CMTY_Weights')
    CMTY_Rho_Cty = getParam('CMTY_Rho_Cty')
    CMTY_Rho_Tenor = getParam('CMTY_Rho_Tenor')
    CMTY_Rho_Basis = getParam('CMTY_Rho_Basis')
    CMTY_Gamma = getParam('CMTY_Gamma')
    CMTY_LH = getParam('CMTY_LH')
    CMTY_vega_rw = getParam('CMTY_vega_rw')
    
    CMTY_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='CMTY')]
    CMTY_RawData['COMM_ASSET'] = CMTY_RawData['RISK_FACTOR_ID'].str.split('&',expand=True)[0]
    CMTY_RawData['COMM_LOCATION'] = CMTY_RawData['RISK_FACTOR_ID'].str.split('&',expand=True)[1]

    CMTY_Position = CMTY_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                  'RISK_FACTOR_BUCKET','COMM_ASSET',
                                  'COMM_LOCATION','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    CMTY_Position = CMTY_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                           'RISK_FACTOR_BUCKET','COMM_ASSET',
                                           'COMM_LOCATION','SENSITIVITY_TYPE']
                                          ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()
    CMTY_vega = CMTY_Position[(CMTY_Position['SENSITIVITY_TYPE']=='Vega')]
    CMTY_vega = CMTY_vega.assign(RISKWEIGHT=CMTY_vega_rw)
    CMTY_vega = CMTY_vega.assign(WEIGHTED_SENSITIVITY=CMTY_vega['SENSITIVITY_VAL_RPT_CURR_CNY'] * CMTY_vega_rw)

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

    CMTY_vega_kl = CMTY_vega_kl.merge(CMTY_Rho_Cty,on='RISK_FACTOR_BUCKET',how='left')

    CMTY_vega_kl.loc[CMTY_vega_kl['COMM_ASSET_K']==CMTY_vega_kl['COMM_ASSET_L'],'Rho_Cty']=1
    CMTY_vega_kl.loc[CMTY_vega_kl['COMM_ASSET_K']!=CMTY_vega_kl['COMM_ASSET_L'],'Rho_Cty']=CMTY_vega_kl['Rho']
    CMTY_vega_kl.loc[CMTY_vega_kl['RISK_FACTOR_VERTEX_1_K']==CMTY_vega_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=1
    CMTY_vega_kl.loc[CMTY_vega_kl['RISK_FACTOR_VERTEX_1_K']!=CMTY_vega_kl['RISK_FACTOR_VERTEX_1_L'],'Rho_Tenor']=CMTY_Rho_Tenor
    CMTY_vega_kl.loc[CMTY_vega_kl['RISK_FACTOR_ID_K']==CMTY_vega_kl['RISK_FACTOR_ID_L'],'Rho_Basis']=1
    CMTY_vega_kl.loc[CMTY_vega_kl['RISK_FACTOR_ID_K']!=CMTY_vega_kl['RISK_FACTOR_ID_L'],'Rho_Basis']=CMTY_Rho_Basis

    CMTY_vega_kl['Rho_kl_delta_M'] = CMTY_vega_kl['Rho_Cty']*CMTY_vega_kl['Rho_Tenor']*CMTY_vega_kl['Rho_Basis']
    CMTY_vega_kl['Rho_kl_opt_mat_M'] = np.exp(
        -0.01*abs(
            CMTY_vega_kl['RISK_FACTOR_VERTEX_1_K']-CMTY_vega_kl['RISK_FACTOR_VERTEX_1_L']
        )/np.minimum(CMTY_vega_kl['RISK_FACTOR_VERTEX_1_K'],CMTY_vega_kl['RISK_FACTOR_VERTEX_1_L']))
    CMTY_vega_kl['Rho_kl_M']=np.minimum((CMTY_vega_kl['Rho_kl_opt_mat_M']*CMTY_vega_kl['Rho_kl_delta_M']),1)

    CMTY_vega_kl['Rho_kl_M']=CMTY_vega_kl['Rho_kl_delta_M']
    CMTY_vega_kl['Rho_kl_H']=np.minimum(1,High_Multipler*CMTY_vega_kl['Rho_kl_M'])
    CMTY_vega_kl['Rho_kl_L']=np.maximum(Low_Multipler1*CMTY_vega_kl['Rho_kl_M']-1,Low_Multipler2*CMTY_vega_kl['Rho_kl_M'])
    CMTY_vega_kl['rslt_kl_M']=CMTY_vega_kl['Rho_kl_M']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_K']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_L']
    CMTY_vega_kl['rslt_kl_H']=CMTY_vega_kl['Rho_kl_H']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_K']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_L']
    CMTY_vega_kl['rslt_kl_L']=CMTY_vega_kl['Rho_kl_L']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_K']*CMTY_vega_kl['WEIGHTED_SENSITIVITY_L']

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

    CMTY_vega_agg = CMTY_vega.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()

    CMTY_vega_bc = CMTY_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                   ).merge(CMTY_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c','WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                           ,on=['RISK_FACTOR_CLASS'],how='left')

    CMTY_vega_bc = CMTY_vega_bc.merge(CMTY_Gamma,on=['Bucket_b','Bucket_c'],how='left').rename({'Gamma_bc':'Gamma_bc_M'},axis=1)
    CMTY_vega_bc['Gamma_bc_H'] = np.minimum(1, CMTY_vega_bc['Gamma_bc_M']*High_Multipler)
    CMTY_vega_bc['Gamma_bc_L'] = np.maximum((Low_Multipler1*CMTY_vega_bc['Gamma_bc_M']-1),(Low_Multipler2*CMTY_vega_bc['Gamma_bc_M']))

    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_M']==1,'rslt_bc_M']=0
    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_M']!=1,'rslt_bc_M']=CMTY_vega_bc.WS_b*CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_M
    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_H']==1,'rslt_bc_H']=0
    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_H']!=1,'rslt_bc_H']=CMTY_vega_bc.WS_b*CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_H
    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_L']==1,'rslt_bc_L']=0
    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_L']!=1,'rslt_bc_L']=CMTY_vega_bc.WS_b*CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_L

    CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']==CMTY_vega_bc['Bucket_c'],'gammac_M']=0
    CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']!=CMTY_vega_bc['Bucket_c'],'gammac_M']=CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_M
    CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']==CMTY_vega_bc['Bucket_c'],'gammac_H']=0
    CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']!=CMTY_vega_bc['Bucket_c'],'gammac_H']=CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_H
    CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']==CMTY_vega_bc['Bucket_c'],'gammac_L']=0
    CMTY_vega_bc.loc[CMTY_vega_bc['Bucket_b']!=CMTY_vega_bc['Bucket_c'],'gammac_L']=CMTY_vega_bc.WS_c*CMTY_vega_bc.Gamma_bc_L

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

    CMTY_vega_agg['Sb*_M']=np.maximum(np.minimum(CMTY_vega_agg['Kb_M'],CMTY_vega_agg['Sb_M']),-CMTY_vega_agg['Kb_M'])
    CMTY_vega_agg['Sb*_H']=np.maximum(np.minimum(CMTY_vega_agg['Kb_H'],CMTY_vega_agg['Sb_H']),-CMTY_vega_agg['Kb_H'])
    CMTY_vega_agg['Sb*_L']=np.maximum(np.minimum(CMTY_vega_agg['Kb_L'],CMTY_vega_agg['Sb_L']),-CMTY_vega_agg['Kb_L'])

    CMTY_vega_bc=CMTY_vega_bc.merge(
        CMTY_vega_agg[['RISK_FACTOR_BUCKET','Sb*_M','Sb*_H','Sb*_L']]
        ,left_on=['Bucket_b'],right_on=['RISK_FACTOR_BUCKET'],how='left')

    CMTY_vega_bc=CMTY_vega_bc.merge(
        CMTY_vega_agg.rename({'Sb*_M':'Sc*_M','Sb*_H':'Sc*_H','Sb*_L':'Sc*_L'},axis=1)[['RISK_FACTOR_BUCKET','Sc*_M','Sc*_H','Sc*_L']]
        ,left_on=['Bucket_c'],right_on=['RISK_FACTOR_BUCKET'],how='left')

    CMTY_vega_bc=CMTY_vega_bc.drop(['RISK_FACTOR_BUCKET_x','RISK_FACTOR_BUCKET_y'],axis=1)

    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_M']==1,'rslt_bc*_M']=0
    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_M']!=1,'rslt_bc*_M']=CMTY_vega_bc['Sb*_M']*CMTY_vega_bc['Sc*_M']*CMTY_vega_bc['Gamma_bc_M']
    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_H']==1,'rslt_bc*_H']=0
    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_H']!=1,'rslt_bc*_H']=CMTY_vega_bc['Sb*_H']*CMTY_vega_bc['Sc*_H']*CMTY_vega_bc['Gamma_bc_H']
    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_L']==1,'rslt_bc*_L']=0
    CMTY_vega_bc.loc[CMTY_vega_bc['Gamma_bc_L']!=1,'rslt_bc*_L']=CMTY_vega_bc['Sb*_L']*CMTY_vega_bc['Sc*_L']*CMTY_vega_bc['Gamma_bc_L']

    cmtyv=pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])

    cmtyv_M_est=sum(CMTY_vega_agg['Kb_M^2'])+sum(CMTY_vega_bc['rslt_bc_M'])
    cmtyv_M_1=np.sqrt(sum(CMTY_vega_agg['Kb_M^2'])+sum(CMTY_vega_bc['rslt_bc_M']))
    cmtyv_M_2=np.sqrt(sum(CMTY_vega_agg['Kb_M^2'])+sum(CMTY_vega_bc['rslt_bc*_M']))

    cmtyv_H_est=sum(CMTY_vega_agg['Kb_H^2'])+sum(CMTY_vega_bc['rslt_bc_H'])
    cmtyv_H_1=np.sqrt(sum(CMTY_vega_agg['Kb_H^2'])+sum(CMTY_vega_bc['rslt_bc_H']))
    cmtyv_H_2=np.sqrt(sum(CMTY_vega_agg['Kb_H^2'])+sum(CMTY_vega_bc['rslt_bc*_H']))

    cmtyv_L_est=sum(CMTY_vega_agg['Kb_L^2'])+sum(CMTY_vega_bc['rslt_bc_L'])
    cmtyv_L_1=np.sqrt(sum(CMTY_vega_agg['Kb_L^2'])+sum(CMTY_vega_bc['rslt_bc_L']))
    cmtyv_L_2=np.sqrt(sum(CMTY_vega_agg['Kb_L^2'])+sum(CMTY_vega_bc['rslt_bc*_L']))

    cmtyv['RISK_FACTOR_CLASS']='CMTY'
    cmtyv['SENS_TYPE']='VEGA'
    cmtyv['NORMAL']=np.where(cmtyv_M_est>=0,cmtyv_M_1,cmtyv_M_2)
    cmtyv['HIGH']=np.where(cmtyv_H_est>=0,cmtyv_H_1,cmtyv_H_2)
    cmtyv['LOW']=np.where(cmtyv_L_est>=0,cmtyv_L_1,cmtyv_L_2)

    cmtyv_1=CMTY_vega[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
    cmtyv_2=CMTY_vega_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                                 ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()
    cmtyv_3=CMTY_vega_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                                 ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    cmtyv_4=CMTY_vega_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]

    cmtyv_decomp=cmtyv_1.merge(cmtyv_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET']
                               ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                               ,how='left')\
    .merge(cmtyv_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(cmtyv_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(cmtyv,on=['RISK_FACTOR_CLASS'],how='left')

    cmtyv_decomp=cmtyv_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','Bucket_b','SENS_TYPE'],axis=1)

    cmtyv_decomp['M_est']=cmtyv_M_est
    cmtyv_decomp['H_est']=cmtyv_H_est
    cmtyv_decomp['L_est']=cmtyv_L_est

    #case 1
    cmtyv_decomp.loc[(cmtyv_decomp['M_est']>=0)&(cmtyv_decomp['Kb_M']>0),'pder_M']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_M']+cmtyv_decomp['gammac_M'])/cmtyv_decomp['NORMAL']
    cmtyv_decomp.loc[(cmtyv_decomp['H_est']>=0)&(cmtyv_decomp['Kb_H']>0),'pder_H']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_H']+cmtyv_decomp['gammac_H'])/cmtyv_decomp['HIGH']
    cmtyv_decomp.loc[(cmtyv_decomp['L_est']>=0)&(cmtyv_decomp['Kb_L']>0),'pder_L']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_L']+cmtyv_decomp['gammac_L'])/cmtyv_decomp['LOW']

    #case 2
    cmtyv_decomp.loc[(cmtyv_decomp['M_est']>=0)&(cmtyv_decomp['Kb_M']==0),'pder_M']=cmtyv_decomp['gammac_M']/cmtyv_decomp['NORMAL']
    cmtyv_decomp.loc[(cmtyv_decomp['H_est']>=0)&(cmtyv_decomp['Kb_H']==0),'pder_H']=cmtyv_decomp['gammac_H']/cmtyv_decomp['HIGH']
    cmtyv_decomp.loc[(cmtyv_decomp['L_est']>=0)&(cmtyv_decomp['Kb_L']==0),'pder_L']=cmtyv_decomp['gammac_L']/cmtyv_decomp['LOW']

    #case 3
    cmtyv_decomp.loc[(cmtyv_decomp['M_est']<0)&(cmtyv_decomp['Kb_M']>0)&(cmtyv_decomp['Sb*_M']==cmtyv_decomp['Kb_M']),'pder_M']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_M'])*(1+1/cmtyv_decomp['Kb_M']*cmtyv_decomp['gammac_M']))/cmtyv_decomp['NORMAL']
    cmtyv_decomp.loc[(cmtyv_decomp['H_est']<0)&(cmtyv_decomp['Kb_H']>0)&(cmtyv_decomp['Sb*_H']==cmtyv_decomp['Kb_H']),'pder_H']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_H'])*(1+1/cmtyv_decomp['Kb_H']*cmtyv_decomp['gammac_H']))/cmtyv_decomp['HIGH']
    cmtyv_decomp.loc[(cmtyv_decomp['L_est']<0)&(cmtyv_decomp['Kb_L']>0)&(cmtyv_decomp['Sb*_L']==cmtyv_decomp['Kb_L']),'pder_L']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_L'])*(1+1/cmtyv_decomp['Kb_L']*cmtyv_decomp['gammac_L']))/cmtyv_decomp['LOW']

    #case 4
    cmtyv_decomp.loc[(cmtyv_decomp['M_est']<0)&(cmtyv_decomp['Kb_M']>0)&(cmtyv_decomp['Sb*_M']+cmtyv_decomp['Kb_M']==0),'pder_M']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_M'])*(1-1/cmtyv_decomp['Kb_M']*cmtyv_decomp['gammac_M']))/cmtyv_decomp['NORMAL']
    cmtyv_decomp.loc[(cmtyv_decomp['H_est']<0)&(cmtyv_decomp['Kb_H']>0)&(cmtyv_decomp['Sb*_H']+cmtyv_decomp['Kb_H']==0),'pder_H']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_H'])*(1-1/cmtyv_decomp['Kb_H']*cmtyv_decomp['gammac_H']))/cmtyv_decomp['HIGH']
    cmtyv_decomp.loc[(cmtyv_decomp['L_est']<0)&(cmtyv_decomp['Kb_L']>0)&(cmtyv_decomp['Sb*_L']+cmtyv_decomp['Kb_L']==0),'pder_L']=((cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_L'])*(1-1/cmtyv_decomp['Kb_L']*cmtyv_decomp['gammac_L']))/cmtyv_decomp['LOW']

    #case 5
    cmtyv_decomp.loc[(cmtyv_decomp['M_est']<0)&(cmtyv_decomp['Kb_M']>0)&(abs(cmtyv_decomp['Sb*_M'])!=abs(cmtyv_decomp['Kb_M'])),'pder_M']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_M']+cmtyv_decomp['gammac_M'])/cmtyv_decomp['NORMAL']
    cmtyv_decomp.loc[(cmtyv_decomp['H_est']<0)&(cmtyv_decomp['Kb_H']>0)&(abs(cmtyv_decomp['Sb*_H'])!=abs(cmtyv_decomp['Kb_H'])),'pder_H']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_H']+cmtyv_decomp['gammac_H'])/cmtyv_decomp['HIGH']
    cmtyv_decomp.loc[(cmtyv_decomp['L_est']<0)&(cmtyv_decomp['Kb_L']>0)&(abs(cmtyv_decomp['Sb*_L'])!=abs(cmtyv_decomp['Kb_L'])),'pder_L']=(cmtyv_decomp['WEIGHTED_SENSITIVITY']+cmtyv_decomp['rhol_L']+cmtyv_decomp['gammac_L'])/cmtyv_decomp['LOW']

    #case 6
    cmtyv_decomp.loc[(cmtyv_decomp['M_est']<0)&(cmtyv_decomp['Kb_M']==0),'pder_M']=0
    cmtyv_decomp.loc[(cmtyv_decomp['H_est']<0)&(cmtyv_decomp['Kb_H']==0),'pder_H']=0
    cmtyv_decomp.loc[(cmtyv_decomp['L_est']<0)&(cmtyv_decomp['Kb_L']==0),'pder_L']=0

    cmtyv_decomp=cmtyv_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]

    cmtyv_decomp_rslt=CMTY_vega.merge(cmtyv_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'],how='left')

    cmtyv_decomp_rslt=cmtyv_decomp_rslt.fillna({'pder_M':0,'pder_H':0,'pder_L':0})

    return CMTY_vega, CMTY_vega_agg, cmtyv, cmtyv_decomp_rslt


# ##### CMTY_Curvature

# In[57]:


def CMTY_Curvature(Raw_Data):
    # get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    CMTY_Weights = getParam('CMTY_Weights')
    CMTY_Rho_Cty = getParam('CMTY_Rho_Cty')
    CMTY_Rho_Tenor = getParam('CMTY_Rho_Tenor')
    CMTY_Rho_Basis = getParam('CMTY_Rho_Basis')
    CMTY_Gamma = getParam('CMTY_Gamma')
    CMTY_LH = getParam('CMTY_LH')
    CMTY_vega_rw = getParam('CMTY_vega_rw')
    
    CMTY_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='CMTY')]
    CMTY_RawData['COMM_ASSET'] = CMTY_RawData['RISK_FACTOR_ID'].str.split('&',expand=True)[0]
    CMTY_RawData['COMM_LOCATION'] = CMTY_RawData['RISK_FACTOR_ID'].str.split('&',expand=True)[1]

    CMTY_Position = CMTY_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                  'RISK_FACTOR_BUCKET','COMM_ASSET',
                                  'COMM_LOCATION','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    CMTY_Position = CMTY_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                           'RISK_FACTOR_BUCKET','COMM_ASSET',
                                           'COMM_LOCATION','SENSITIVITY_TYPE']
                                          ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()
    CMTY_curvature = CMTY_Position.query('SENSITIVITY_TYPE=="Curvature Up"|SENSITIVITY_TYPE=="Curvature Down"')

    CMTY_curvature = CMTY_curvature.groupby(['RISK_FACTOR_ID','RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE']
                                          ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    CMTY_curvature = CMTY_curvature.assign(max_0_square=np.square(np.maximum(CMTY_curvature['SENSITIVITY_VAL_RPT_CURR_CNY'],0)))
    CMTY_curvature = CMTY_curvature.assign(WEIGHTED_SENSITIVITY=CMTY_curvature['SENSITIVITY_VAL_RPT_CURR_CNY'])

    CMTY_curvature_agg = CMTY_curvature.groupby(
        ['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],dropna=False
    ).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum','max_0_square':'sum'}).reset_index()

    CMTY_curvature_agg['max_0_k']=np.sqrt(CMTY_curvature_agg['max_0_square'])

    CMTY_curvature_agg=CMTY_curvature_agg.pivot(index=('RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET')
                             ,columns='SENSITIVITY_TYPE')

    CMTY_curvature_agg.columns=['/'.join(i) for i in CMTY_curvature_agg.columns]
    CMTY_curvature_agg=CMTY_curvature_agg.reset_index()

    CMTY_curvature_agg['Kb+_M']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Up'])))
    CMTY_curvature_agg['Kb-_M']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Down'])))
    CMTY_curvature_agg['Kb_M']=np.maximum(CMTY_curvature_agg['Kb+_M'],CMTY_curvature_agg['Kb-_M'])
    CMTY_curvature_agg['Kb_M^2']=np.square(CMTY_curvature_agg['Kb_M'])
    CMTY_curvature_agg['Sb_M']=np.select([(CMTY_curvature_agg['Kb_M'] == CMTY_curvature_agg['Kb+_M']),
                                          (CMTY_curvature_agg['Kb_M'] != CMTY_curvature_agg['Kb+_M'])],
                                         [(CMTY_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (CMTY_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    CMTY_curvature_agg['Kb+_H']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Up'])))
    CMTY_curvature_agg['Kb-_H']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Down'])))
    CMTY_curvature_agg['Kb_H']=np.maximum(CMTY_curvature_agg['Kb+_H'],CMTY_curvature_agg['Kb-_H'])
    CMTY_curvature_agg['Kb_H^2']=np.square(CMTY_curvature_agg['Kb_H'])
    CMTY_curvature_agg['Sb_H']=np.select([(CMTY_curvature_agg['Kb_H'] == CMTY_curvature_agg['Kb+_H']),
                                          (CMTY_curvature_agg['Kb_H'] != CMTY_curvature_agg['Kb+_H'])],
                                         [(CMTY_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (CMTY_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    CMTY_curvature_agg['Kb+_L']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Up'])))
    CMTY_curvature_agg['Kb-_L']=np.sqrt(np.maximum(0,(CMTY_curvature_agg['max_0_square/Curvature Down'])))
    CMTY_curvature_agg['Kb_L']=np.maximum(CMTY_curvature_agg['Kb+_L'],CMTY_curvature_agg['Kb-_L'])
    CMTY_curvature_agg['Kb_L^2']=np.square(CMTY_curvature_agg['Kb_L'])
    CMTY_curvature_agg['Sb_L']=np.select([(CMTY_curvature_agg['Kb_L'] == CMTY_curvature_agg['Kb+_L']),
                                          (CMTY_curvature_agg['Kb_L'] != CMTY_curvature_agg['Kb+_L'])],
                                         [(CMTY_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (CMTY_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    CMTY_curvature_agg['max']=np.select([(CMTY_curvature_agg['Kb_M'] == CMTY_curvature_agg['Kb+_M']),
                                         (CMTY_curvature_agg['Kb_M'] != CMTY_curvature_agg['Kb+_M'])],
                                        [(CMTY_curvature_agg['max_0_k/Curvature Up']),
                                         (CMTY_curvature_agg['max_0_k/Curvature Down'])])

    CMTY_curvature_agg['sign']=np.select([(CMTY_curvature_agg['Kb_M'] == CMTY_curvature_agg['Kb+_M']),
                                          (CMTY_curvature_agg['Kb_M'] != CMTY_curvature_agg['Kb+_M'])],
                                         ['Curvature Up','Curvature Down'])

    CMTY_curvature_bc=CMTY_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Sb_M']]
    CMTY_curvature_bc=CMTY_curvature_bc.rename(
        {'Sb_M':'Sb','RISK_FACTOR_BUCKET':'Bucket_b'},axis=1
    ).merge(CMTY_curvature_bc.rename(
        {'Sb_M':'Sc','RISK_FACTOR_BUCKET':'Bucket_c'},axis=1
    ),on=['RISK_FACTOR_CLASS'],how='left')

    CMTY_curvature_bc.loc[(CMTY_curvature_bc['Sb']<0) & (CMTY_curvature_bc['Sc']<0),'Psi']=0
    CMTY_curvature_bc.loc[(CMTY_curvature_bc['Sb']>=0) | (CMTY_curvature_bc['Sc']>=0),'Psi']=1
    CMTY_curvature_bc=CMTY_curvature_bc.merge(CMTY_Gamma,on=['Bucket_b','Bucket_c'],how='left')
    CMTY_curvature_bc['Gamma_bc_M']=np.square(CMTY_curvature_bc['Gamma_bc'])

    CMTY_curvature_bc['Gamma_bc_H']=np.square(np.minimum(1,CMTY_curvature_bc['Gamma_bc']*High_Multipler))
    CMTY_curvature_bc['Gamma_bc_L']=np.square(np.maximum((Low_Multipler1*CMTY_curvature_bc['Gamma_bc']-1),(Low_Multipler2*CMTY_curvature_bc['Gamma_bc'])))
    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'rslt_bc_M']=0
    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'rslt_bc_M']=CMTY_curvature_bc['Gamma_bc_M']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sb']*CMTY_curvature_bc['Sc']
    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'rslt_bc_H']=0
    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'rslt_bc_H']=CMTY_curvature_bc['Gamma_bc_H']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sb']*CMTY_curvature_bc['Sc']
    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'rslt_bc_L']=0
    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'rslt_bc_L']=CMTY_curvature_bc['Gamma_bc_L']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sb']*CMTY_curvature_bc['Sc']

    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'gammac_M']=0
    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'gammac_M']=CMTY_curvature_bc['Gamma_bc_M']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sc']
    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'gammac_H']=0
    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'gammac_H']=CMTY_curvature_bc['Gamma_bc_H']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sc']
    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b==CMTY_curvature_bc.Bucket_c),'gammac_L']=0
    CMTY_curvature_bc.loc[(CMTY_curvature_bc.Bucket_b!=CMTY_curvature_bc.Bucket_c),'gammac_L']=CMTY_curvature_bc['Gamma_bc_L']*CMTY_curvature_bc['Psi']*CMTY_curvature_bc['Sc']

    cmtyc_M_est=sum(CMTY_curvature_agg['Kb_M^2'])+sum(CMTY_curvature_bc['rslt_bc_M'])
    cmtyc_H_est=sum(CMTY_curvature_agg['Kb_H^2'])+sum(CMTY_curvature_bc['rslt_bc_H'])
    cmtyc_L_est=sum(CMTY_curvature_agg['Kb_L^2'])+sum(CMTY_curvature_bc['rslt_bc_L'])

    cmtyc_M = np.sqrt(np.maximum(0,sum(CMTY_curvature_agg['Kb_M^2'])+sum(CMTY_curvature_bc['rslt_bc_M'])))
    cmtyc_H = np.sqrt(np.maximum(0,sum(CMTY_curvature_agg['Kb_H^2'])+sum(CMTY_curvature_bc['rslt_bc_H'])))
    cmtyc_L = np.sqrt(np.maximum(0,sum(CMTY_curvature_agg['Kb_L^2'])+sum(CMTY_curvature_bc['rslt_bc_L'])))

    cmtyc=pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])

    cmtyc['RISK_FACTOR_CLASS']='CMTY'
    cmtyc['SENS_TYPE']='CURVATURE'
    cmtyc['NORMAL']=cmtyc_M
    cmtyc['HIGH']=cmtyc_H
    cmtyc['LOW']=cmtyc_L

    cmtyc_1=CMTY_curvature[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]
    cmtyc_3=CMTY_curvature_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                                      ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    cmtyc_4=CMTY_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','max','sign']]

    cmtyc_decomp=cmtyc_1.merge(cmtyc_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET']
                               ,right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(cmtyc_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(cmtyc,on=['RISK_FACTOR_CLASS'],how='left')

    cmtyc_decomp=cmtyc_decomp.drop(['Bucket_b','SENS_TYPE'],axis=1)

    cmtyc_decomp['M_est']=cmtyc_M_est
    cmtyc_decomp['H_est']=cmtyc_H_est
    cmtyc_decomp['L_est']=cmtyc_L_est

    cmtyc_decomp=cmtyc_decomp[(cmtyc_decomp.SENSITIVITY_TYPE==cmtyc_decomp.sign)]

    #case 1/2
    cmtyc_decomp.loc[(cmtyc_decomp['M_est']>=0),'pder_M']=(cmtyc_decomp['max']+cmtyc_decomp['gammac_M'])/cmtyc_decomp['NORMAL']
    cmtyc_decomp.loc[(cmtyc_decomp['H_est']>=0),'pder_H']=(cmtyc_decomp['max']+cmtyc_decomp['gammac_H'])/cmtyc_decomp['HIGH']
    cmtyc_decomp.loc[(cmtyc_decomp['L_est']>=0),'pder_L']=(cmtyc_decomp['max']+cmtyc_decomp['gammac_L'])/cmtyc_decomp['LOW']

    #case 3 
    cmtyc_decomp.loc[(cmtyc_decomp['M_est']<0),'pder_M']=0
    cmtyc_decomp.loc[(cmtyc_decomp['H_est']<0),'pder_H']=0
    cmtyc_decomp.loc[(cmtyc_decomp['L_est']<0),'pder_L']=0

    cmtyc_decomp=cmtyc_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','pder_M','pder_H','pder_L']]

    cmtyc_decomp_rslt=CMTY_curvature.merge(cmtyc_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],how='right')

    return CMTY_curvature, CMTY_curvature_agg, cmtyc, cmtyc_decomp_rslt


# #### FX

# ##### FX_Delta

# In[60]:


def FX_Delta(Raw_Data):
    # get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    FX_Weights = getParam('FX_Weights')
    FX_Gamma = getParam('FX_Gamma')
    FX_LH = getParam('FX_LH')
    FX_vega_rw = getParam('FX_vega_rw')
    
    FX_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='FX')]

    FX_Position = FX_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                              'RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    FX_Position = FX_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                       'RISK_FACTOR_BUCKET','SENSITIVITY_TYPE']
                                      ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    FX_delta = FX_Position[(FX_Position['SENSITIVITY_TYPE']=='Delta')]
    FX_delta = FX_delta.merge(FX_Weights,on='RISK_FACTOR_BUCKET',how='left')
    FX_delta = FX_delta.rename({'Risk_Weight':'RISKWEIGHT'},axis=1)
    FX_delta['WEIGHTED_SENSITIVITY']=FX_delta['SENSITIVITY_VAL_RPT_CURR_CNY']*FX_delta['RISKWEIGHT']

    FX_delta_agg=FX_delta.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()

    FX_delta_bc=FX_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                                       ).merge(FX_delta_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c'
                                                                      ,'WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                                               ,on =['RISK_FACTOR_CLASS'],how='left')
    #FX_delta_bc = FX_delta_bc.loc[(FX_delta_bc.Bucket_b!=FX_delta_bc.Bucket_c),:].reset_index(drop=True)
    FX_delta_bc.loc[(FX_delta_bc.Bucket_b!=FX_delta_bc.Bucket_c),'Gamma_bc_M']=FX_Gamma
    FX_delta_bc.loc[(FX_delta_bc.Bucket_b==FX_delta_bc.Bucket_c),'Gamma_bc_M']=0
    
    FX_delta_bc['Gamma_bc_H']=np.minimum(1,High_Multipler*FX_delta_bc['Gamma_bc_M'])
    FX_delta_bc['Gamma_bc_L']=np.maximum(Low_Multipler1*FX_delta_bc['Gamma_bc_M']-1,Low_Multipler2*FX_delta_bc['Gamma_bc_M'])

    FX_delta_bc['rslt_bc_M']=FX_delta_bc['WS_b']*FX_delta_bc['WS_c']*FX_delta_bc['Gamma_bc_M']
    FX_delta_bc['rslt_bc_H']=FX_delta_bc['WS_b']*FX_delta_bc['WS_c']*FX_delta_bc['Gamma_bc_H']
    FX_delta_bc['rslt_bc_L']=FX_delta_bc['WS_b']*FX_delta_bc['WS_c']*FX_delta_bc['Gamma_bc_L']

    FX_delta_bc['gammac_M']=FX_delta_bc.WS_c*FX_delta_bc.Gamma_bc_M
    FX_delta_bc['gammac_H']=FX_delta_bc.WS_c*FX_delta_bc.Gamma_bc_H
    FX_delta_bc['gammac_L']=FX_delta_bc.WS_c*FX_delta_bc.Gamma_bc_L

    FX_delta_agg['Sb_M']=FX_delta_agg['Sb_H']=FX_delta_agg['Sb_L']=FX_delta_agg['WEIGHTED_SENSITIVITY']
    FX_delta_agg['Kb_M']=FX_delta_agg['Kb_H']=FX_delta_agg['Kb_L']=np.sqrt(np.maximum(0,np.square(FX_delta_agg['WEIGHTED_SENSITIVITY'])))
    FX_delta_agg['Kb_M^2']=np.square(FX_delta_agg['Kb_M'])
    FX_delta_agg['Kb_H^2']=np.square(FX_delta_agg['Kb_H'])
    FX_delta_agg['Kb_L^2']=np.square(FX_delta_agg['Kb_L'])

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

    FX_delta_bc['rslt_bc*_M']=FX_delta_bc['Sb*_M']*FX_delta_bc['Sc*_M']*FX_delta_bc['Gamma_bc_M']
    FX_delta_bc['rslt_bc*_H']=FX_delta_bc['Sb*_H']*FX_delta_bc['Sc*_H']*FX_delta_bc['Gamma_bc_H']
    FX_delta_bc['rslt_bc*_L']=FX_delta_bc['Sb*_L']*FX_delta_bc['Sc*_L']*FX_delta_bc['Gamma_bc_L']

    fxd = pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=['0'])

    fxd_M_est=sum(FX_delta_agg['Kb_M^2'])+sum(FX_delta_bc['rslt_bc_M'])
    fxd_M_1=np.sqrt(sum(FX_delta_agg['Kb_M^2'])+sum(FX_delta_bc['rslt_bc_M']))
    fxd_M_2=np.sqrt(sum(FX_delta_agg['Kb_M^2'])+sum(FX_delta_bc['rslt_bc*_M']))

    fxd_H_est=sum(FX_delta_agg['Kb_H^2'])+sum(FX_delta_bc['rslt_bc_H'])
    fxd_H_1=np.sqrt(sum(FX_delta_agg['Kb_H^2'])+sum(FX_delta_bc['rslt_bc_H']))
    fxd_H_2=np.sqrt(sum(FX_delta_agg['Kb_H^2'])+sum(FX_delta_bc['rslt_bc*_H']))

    fxd_L_est=sum(FX_delta_agg['Kb_L^2'])+sum(FX_delta_bc['rslt_bc_L'])
    fxd_L_1=np.sqrt(sum(FX_delta_agg['Kb_L^2'])+sum(FX_delta_bc['rslt_bc_L']))
    fxd_L_2=np.sqrt(sum(FX_delta_agg['Kb_L^2'])+sum(FX_delta_bc['rslt_bc*_L']))

    fxd['RISK_FACTOR_CLASS']='FX'
    fxd['SENS_TYPE']='DELTA'
    fxd['NORMAL']=np.where(fxd_M_est>=0,fxd_M_1,fxd_M_2)
    fxd['HIGH']=np.where(fxd_H_est>=0,fxd_H_1,fxd_H_2)
    fxd['LOW']=np.where(fxd_L_est>=0,fxd_L_1,fxd_L_2)

    fxd_1=FX_delta[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
    fxd_3=FX_delta_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                          ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    fxd_4=FX_delta_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]

    fxd_decomp=fxd_1.merge(fxd_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left').merge(fxd_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left').merge(fxd,on=['RISK_FACTOR_CLASS'],how='left')

    fxd_decomp=fxd_decomp.drop(['Bucket_b','SENS_TYPE'],axis=1)

    fxd_decomp['M_est']=fxd_M_est
    fxd_decomp['H_est']=fxd_H_est
    fxd_decomp['L_est']=fxd_L_est

    #case 1
    fxd_decomp.loc[(fxd_decomp['M_est']>=0)&(fxd_decomp['Kb_M']>0),'pder_M']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_M'])/fxd_decomp['NORMAL']
    fxd_decomp.loc[(fxd_decomp['H_est']>=0)&(fxd_decomp['Kb_H']>0),'pder_H']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_H'])/fxd_decomp['HIGH']
    fxd_decomp.loc[(fxd_decomp['L_est']>=0)&(fxd_decomp['Kb_L']>0),'pder_L']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_L'])/fxd_decomp['LOW']

    #case 2
    fxd_decomp.loc[(fxd_decomp['M_est']>=0)&(fxd_decomp['Kb_M']==0),'pder_M']=fxd_decomp['gammac_M']/fxd_decomp['NORMAL']
    fxd_decomp.loc[(fxd_decomp['H_est']>=0)&(fxd_decomp['Kb_H']==0),'pder_H']=fxd_decomp['gammac_H']/fxd_decomp['HIGH']
    fxd_decomp.loc[(fxd_decomp['L_est']>=0)&(fxd_decomp['Kb_L']==0),'pder_L']=fxd_decomp['gammac_L']/fxd_decomp['LOW']

    #case 3
    fxd_decomp.loc[(fxd_decomp['M_est']<0)&(fxd_decomp['Kb_M']>0)&(fxd_decomp['Sb*_M']==fxd_decomp['Kb_M']),'pder_M']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1+1/fxd_decomp['Kb_M']*fxd_decomp['gammac_M']))/fxd_decomp['NORMAL']
    fxd_decomp.loc[(fxd_decomp['H_est']<0)&(fxd_decomp['Kb_H']>0)&(fxd_decomp['Sb*_H']==fxd_decomp['Kb_H']),'pder_H']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1+1/fxd_decomp['Kb_H']*fxd_decomp['gammac_H']))/fxd_decomp['HIGH']
    fxd_decomp.loc[(fxd_decomp['L_est']<0)&(fxd_decomp['Kb_L']>0)&(fxd_decomp['Sb*_L']==fxd_decomp['Kb_L']),'pder_L']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1+1/fxd_decomp['Kb_L']*fxd_decomp['gammac_L']))/fxd_decomp['LOW']

    #case 4
    fxd_decomp.loc[(fxd_decomp['M_est']<0)&(fxd_decomp['Kb_M']>0)&(fxd_decomp['Sb*_M']+fxd_decomp['Kb_M']==0),'pder_M']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1-1/fxd_decomp['Kb_M']*fxd_decomp['gammac_M']))/fxd_decomp['NORMAL']
    fxd_decomp.loc[(fxd_decomp['H_est']<0)&(fxd_decomp['Kb_H']>0)&(fxd_decomp['Sb*_H']+fxd_decomp['Kb_H']==0),'pder_H']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1-1/fxd_decomp['Kb_H']*fxd_decomp['gammac_H']))/fxd_decomp['HIGH']
    fxd_decomp.loc[(fxd_decomp['L_est']<0)&(fxd_decomp['Kb_L']>0)&(fxd_decomp['Sb*_L']+fxd_decomp['Kb_L']==0),'pder_L']=((fxd_decomp['WEIGHTED_SENSITIVITY'])*(1-1/fxd_decomp['Kb_L']*fxd_decomp['gammac_L']))/fxd_decomp['LOW']

    #case 5
    fxd_decomp.loc[(fxd_decomp['M_est']<0)&(fxd_decomp['Kb_M']>0)&(abs(fxd_decomp['Sb*_M'])!=abs(fxd_decomp['Kb_M'])),'pder_M']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_M'])/fxd_decomp['NORMAL']
    fxd_decomp.loc[(fxd_decomp['H_est']<0)&(fxd_decomp['Kb_H']>0)&(abs(fxd_decomp['Sb*_H'])!=abs(fxd_decomp['Kb_H'])),'pder_H']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_H'])/fxd_decomp['HIGH']
    fxd_decomp.loc[(fxd_decomp['L_est']<0)&(fxd_decomp['Kb_L']>0)&(abs(fxd_decomp['Sb*_L'])!=abs(fxd_decomp['Kb_L'])),'pder_L']=(fxd_decomp['WEIGHTED_SENSITIVITY']+fxd_decomp['gammac_L'])/fxd_decomp['LOW']

    #case 6
    fxd_decomp.loc[(fxd_decomp['M_est']<0)&(fxd_decomp['Kb_M']==0),'pder_M']=0
    fxd_decomp.loc[(fxd_decomp['H_est']<0)&(fxd_decomp['Kb_H']==0),'pder_H']=0
    fxd_decomp.loc[(fxd_decomp['L_est']<0)&(fxd_decomp['Kb_L']==0),'pder_L']=0

    fxd_decomp=fxd_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]

    fxd_decomp_rslt=FX_delta.merge(fxd_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET'],how='left')

    return FX_delta, FX_delta_agg, fxd, fxd_decomp_rslt


# ##### FX_Vega

# In[62]:


def FX_Vega(Raw_Data):
    # get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    FX_Weights = getParam('FX_Weights')
    FX_Gamma = getParam('FX_Gamma')
    FX_LH = getParam('FX_LH')
    FX_vega_rw = getParam('FX_vega_rw')
    
    FX_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='FX')]

    FX_Position = FX_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                              'RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    FX_Position = FX_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                       'RISK_FACTOR_BUCKET','SENSITIVITY_TYPE']
                                      ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()

    FX_vega = FX_Position[(FX_Position['SENSITIVITY_TYPE']=='Vega')]
    FX_vega = FX_vega.assign(RISKWEIGHT=FX_vega_rw)
    FX_vega = FX_vega.assign(WEIGHTED_SENSITIVITY=FX_vega['SENSITIVITY_VAL_RPT_CURR_CNY']*FX_vega['RISKWEIGHT'])

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

    FX_vega_kl['Rho_kl_opt_mat_M'] = np.exp(
        -0.01*abs(
            FX_vega_kl['RISK_FACTOR_VERTEX_1_K']-FX_vega_kl['RISK_FACTOR_VERTEX_1_L']
        )/np.minimum(FX_vega_kl['RISK_FACTOR_VERTEX_1_K'],FX_vega_kl['RISK_FACTOR_VERTEX_1_L']))

    FX_vega_kl['Rho_kl_M']=np.minimum(FX_vega_kl['Rho_kl_opt_mat_M'],1)
    FX_vega_kl['rslt_kl_M']=FX_vega_kl['Rho_kl_M']*FX_vega_kl['WEIGHTED_SENSITIVITY_K']*FX_vega_kl['WEIGHTED_SENSITIVITY_L']
    FX_vega_kl['Rho_kl_H']=np.minimum(1,High_Multipler*FX_vega_kl['Rho_kl_M'])
    FX_vega_kl['rslt_kl_H']=FX_vega_kl['Rho_kl_H']*FX_vega_kl['WEIGHTED_SENSITIVITY_K']*FX_vega_kl['WEIGHTED_SENSITIVITY_L']
    FX_vega_kl['Rho_kl_L']=np.maximum(Low_Multipler1*FX_vega_kl['Rho_kl_M']-1,Low_Multipler2*FX_vega_kl['Rho_kl_M'])
    FX_vega_kl['rslt_kl_L']=FX_vega_kl['Rho_kl_L']*FX_vega_kl['WEIGHTED_SENSITIVITY_K']*FX_vega_kl['WEIGHTED_SENSITIVITY_L']

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

    FX_vega_agg=FX_vega.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],dropna=False).agg({'WEIGHTED_SENSITIVITY':'sum'}).reset_index()

    FX_vega_bc=FX_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_b','WEIGHTED_SENSITIVITY':'WS_b'},axis=1
                                       ).merge(FX_vega_agg.rename({'RISK_FACTOR_BUCKET':'Bucket_c'
                                                                      ,'WEIGHTED_SENSITIVITY':'WS_c'},axis=1)
                                               ,on =['RISK_FACTOR_CLASS'],how='left')

    #FX_vega_bc = FX_vega_bc.loc[(FX_vega_bc.Bucket_b!=FX_vega_bc.Bucket_c),:].reset_index(drop=True)

    FX_vega_bc.loc[(FX_vega_bc.Bucket_b!=FX_vega_bc.Bucket_c),'Gamma_bc_M']=FX_Gamma
    FX_vega_bc.loc[(FX_vega_bc.Bucket_b==FX_vega_bc.Bucket_c),'Gamma_bc_M']=0
    FX_vega_bc['Gamma_bc_H']=np.minimum(1,High_Multipler*FX_vega_bc['Gamma_bc_M'])
    FX_vega_bc['Gamma_bc_L']=np.maximum(Low_Multipler1*FX_vega_bc['Gamma_bc_M']-1,Low_Multipler2*FX_vega_bc['Gamma_bc_M'])

    FX_vega_bc['rslt_bc_M']=FX_vega_bc['WS_b']*FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_M']
    FX_vega_bc['rslt_bc_H']=FX_vega_bc['WS_b']*FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_H']
    FX_vega_bc['rslt_bc_L']=FX_vega_bc['WS_b']*FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_L']

    FX_vega_bc['gammac_M']=FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_M']
    FX_vega_bc['gammac_H']=FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_H']
    FX_vega_bc['gammac_L']=FX_vega_bc['WS_c']*FX_vega_bc['Gamma_bc_L']

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

    fxv=pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])

    fxv_M_est=sum(FX_vega_agg['Kb_M^2'])+sum(FX_vega_bc['rslt_bc_M'])
    fxv_M_1=np.sqrt(sum(FX_vega_agg['Kb_M^2'])+sum(FX_vega_bc['rslt_bc_M']))
    fxv_M_2=np.sqrt(sum(FX_vega_agg['Kb_M^2'])+sum(FX_vega_bc['rslt_bc*_M']))

    fxv_H_est=sum(FX_vega_agg['Kb_H^2'])+sum(FX_vega_bc['rslt_bc_H'])
    fxv_H_1=np.sqrt(sum(FX_vega_agg['Kb_H^2'])+sum(FX_vega_bc['rslt_bc_H']))
    fxv_H_2=np.sqrt(sum(FX_vega_agg['Kb_H^2'])+sum(FX_vega_bc['rslt_bc*_H']))

    fxv_L_est=sum(FX_vega_agg['Kb_L^2'])+sum(FX_vega_bc['rslt_bc_L'])
    fxv_L_1=np.sqrt(sum(FX_vega_agg['Kb_L^2'])+sum(FX_vega_bc['rslt_bc_L']))
    fxv_L_2=np.sqrt(sum(FX_vega_agg['Kb_L^2'])+sum(FX_vega_bc['rslt_bc*_L']))

    fxv['RISK_FACTOR_CLASS']='FX'
    fxv['SENS_TYPE']='VEGA'
    fxv['NORMAL']=np.where(fxv_M_est>=0,fxv_M_1,fxv_M_2)
    fxv['HIGH']=np.where(fxv_H_est>=0,fxv_H_1,fxv_H_2)
    fxv['LOW']=np.where(fxv_L_est>=0,fxv_L_1,fxv_L_2)

    fxv_1=FX_vega[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','WEIGHTED_SENSITIVITY']]
    fxv_2=FX_vega_kl.groupby(['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                                 ,dropna=False).agg({'rhol_M':'sum','rhol_H':'sum','rhol_L':'sum'}).reset_index()
    fxv_3=FX_vega_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                                 ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    fxv_4=FX_vega_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','Sb_M','Sb_H','Sb_L','Sb*_M','Sb*_H','Sb*_L']]

    fxv_decomp=fxv_1.merge(fxv_2,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET']
                               ,right_on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','RISK_FACTOR_BUCKET']
                               ,how='left')\
    .merge(fxv_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(fxv_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(fxv,on=['RISK_FACTOR_CLASS'],how='left')

    fxv_decomp=fxv_decomp.drop(['RISK_FACTOR_ID_K','RISK_FACTOR_VERTEX_1_K','Bucket_b','SENS_TYPE'],axis=1)

    fxv_decomp['M_est']=fxv_M_est
    fxv_decomp['H_est']=fxv_H_est
    fxv_decomp['L_est']=fxv_L_est

    #case 1
    fxv_decomp.loc[(fxv_decomp['M_est']>=0)&(fxv_decomp['Kb_M']>0),'pder_M']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_M']+fxv_decomp['gammac_M'])/fxv_decomp['NORMAL']
    fxv_decomp.loc[(fxv_decomp['H_est']>=0)&(fxv_decomp['Kb_H']>0),'pder_H']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_H']+fxv_decomp['gammac_H'])/fxv_decomp['HIGH']
    fxv_decomp.loc[(fxv_decomp['L_est']>=0)&(fxv_decomp['Kb_L']>0),'pder_L']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_L']+fxv_decomp['gammac_L'])/fxv_decomp['LOW']

    #case 2
    fxv_decomp.loc[(fxv_decomp['M_est']>=0)&(fxv_decomp['Kb_M']==0),'pder_M']=fxv_decomp['gammac_M']/fxv_decomp['NORMAL']
    fxv_decomp.loc[(fxv_decomp['H_est']>=0)&(fxv_decomp['Kb_H']==0),'pder_H']=fxv_decomp['gammac_H']/fxv_decomp['HIGH']
    fxv_decomp.loc[(fxv_decomp['L_est']>=0)&(fxv_decomp['Kb_L']==0),'pder_L']=fxv_decomp['gammac_L']/fxv_decomp['LOW']

    #case 3
    fxv_decomp.loc[(fxv_decomp['M_est']<0)&(fxv_decomp['Kb_M']>0)&(fxv_decomp['Sb*_M']==fxv_decomp['Kb_M']),'pder_M']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_M'])*(1+1/fxv_decomp['Kb_M']*fxv_decomp['gammac_M']))/fxv_decomp['NORMAL']
    fxv_decomp.loc[(fxv_decomp['H_est']<0)&(fxv_decomp['Kb_H']>0)&(fxv_decomp['Sb*_H']==fxv_decomp['Kb_H']),'pder_H']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_H'])*(1+1/fxv_decomp['Kb_H']*fxv_decomp['gammac_H']))/fxv_decomp['HIGH']
    fxv_decomp.loc[(fxv_decomp['L_est']<0)&(fxv_decomp['Kb_L']>0)&(fxv_decomp['Sb*_L']==fxv_decomp['Kb_L']),'pder_L']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_L'])*(1+1/fxv_decomp['Kb_L']*fxv_decomp['gammac_L']))/fxv_decomp['LOW']

    #case 4
    fxv_decomp.loc[(fxv_decomp['M_est']<0)&(fxv_decomp['Kb_M']>0)&(fxv_decomp['Sb*_M']+fxv_decomp['Kb_M']==0),'pder_M']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_M'])*(1-1/fxv_decomp['Kb_M']*fxv_decomp['gammac_M']))/fxv_decomp['NORMAL']
    fxv_decomp.loc[(fxv_decomp['H_est']<0)&(fxv_decomp['Kb_H']>0)&(fxv_decomp['Sb*_H']+fxv_decomp['Kb_H']==0),'pder_H']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_H'])*(1-1/fxv_decomp['Kb_H']*fxv_decomp['gammac_H']))/fxv_decomp['HIGH']
    fxv_decomp.loc[(fxv_decomp['L_est']<0)&(fxv_decomp['Kb_L']>0)&(fxv_decomp['Sb*_L']+fxv_decomp['Kb_L']==0),'pder_L']=((fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_L'])*(1-1/fxv_decomp['Kb_L']*fxv_decomp['gammac_L']))/fxv_decomp['LOW']

    #case 5
    fxv_decomp.loc[(fxv_decomp['M_est']<0)&(fxv_decomp['Kb_M']>0)&(abs(fxv_decomp['Sb*_M'])!=abs(fxv_decomp['Kb_M'])),'pder_M']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_M']+fxv_decomp['gammac_M'])/fxv_decomp['NORMAL']
    fxv_decomp.loc[(fxv_decomp['H_est']<0)&(fxv_decomp['Kb_H']>0)&(abs(fxv_decomp['Sb*_H'])!=abs(fxv_decomp['Kb_H'])),'pder_H']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_H']+fxv_decomp['gammac_H'])/fxv_decomp['HIGH']
    fxv_decomp.loc[(fxv_decomp['L_est']<0)&(fxv_decomp['Kb_L']>0)&(abs(fxv_decomp['Sb*_L'])!=abs(fxv_decomp['Kb_L'])),'pder_L']=(fxv_decomp['WEIGHTED_SENSITIVITY']+fxv_decomp['rhol_L']+fxv_decomp['gammac_L'])/fxv_decomp['LOW']

    #case 6
    fxv_decomp.loc[(fxv_decomp['M_est']<0)&(fxv_decomp['Kb_M']==0),'pder_M']=0
    fxv_decomp.loc[(fxv_decomp['H_est']<0)&(fxv_decomp['Kb_H']==0),'pder_H']=0
    fxv_decomp.loc[(fxv_decomp['L_est']<0)&(fxv_decomp['Kb_L']==0),'pder_L']=0

    fxv_decomp=fxv_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET','pder_M','pder_H','pder_L']]

    fxv_decomp_rslt=FX_vega.merge(fxv_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_BUCKET'],how='left')

    fxv_decomp_rslt=fxv_decomp_rslt.fillna({'pder_M':0,'pder_H':0,'pder_L':0})

    return FX_vega, FX_vega_agg, fxv, fxv_decomp_rslt


# ##### FX_Curvature

# In[63]:


def FX_Curvature(Raw_Data):
    # get params:
    High_Multipler = getParam('High_Multipler')
    Low_Multipler1 = getParam('Low_Multipler1')
    Low_Multipler2 = getParam('Low_Multipler2')
    FX_Weights = getParam('FX_Weights')
    FX_Gamma = getParam('FX_Gamma')
    FX_LH = getParam('FX_LH')
    FX_vega_rw = getParam('FX_vega_rw')
    
    FX_RawData = Raw_Data[(Raw_Data['RISK_FACTOR_CLASS']=='FX')]

    FX_Position = FX_RawData[['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                              'RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','SENSITIVITY_VAL_RPT_CURR_CNY']]

    FX_Position = FX_Position.groupby(['RISK_FACTOR_ID','RISK_FACTOR_VERTEX_1','RISK_FACTOR_CLASS',
                                       'RISK_FACTOR_BUCKET','SENSITIVITY_TYPE']
                                      ,dropna=False).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum'}).reset_index()
    FX_curvature = FX_Position.query('SENSITIVITY_TYPE=="Curvature Up"|SENSITIVITY_TYPE=="Curvature Down"')

    FX_curvature = FX_curvature.assign(WEIGHTED_SENSITIVITY=FX_curvature['SENSITIVITY_VAL_RPT_CURR_CNY'])
    FX_curvature = FX_curvature.assign(max_0_square=np.square(np.maximum(FX_curvature['SENSITIVITY_VAL_RPT_CURR_CNY'],0)))

    FX_curvature_agg = FX_curvature.groupby(
        ['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],dropna=False
    ).agg({'SENSITIVITY_VAL_RPT_CURR_CNY':'sum','max_0_square':'sum'}).reset_index()

    FX_curvature_agg['max_0_k']=np.sqrt(FX_curvature_agg['max_0_square'])

    FX_curvature_agg=FX_curvature_agg.pivot(index=('RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET')
                             ,columns='SENSITIVITY_TYPE')

    FX_curvature_agg.columns=['/'.join(i) for i in FX_curvature_agg.columns]
    FX_curvature_agg=FX_curvature_agg.reset_index()

    FX_curvature_agg['Kb+_M']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Up'])))
    FX_curvature_agg['Kb-_M']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Down'])))
    FX_curvature_agg['Kb_M']=np.maximum(FX_curvature_agg['Kb+_M'],FX_curvature_agg['Kb-_M'])
    FX_curvature_agg['Kb_M^2']=np.square(FX_curvature_agg['Kb_M'])
    FX_curvature_agg['Sb_M']=np.select([(FX_curvature_agg['Kb_M'] == FX_curvature_agg['Kb+_M']),
                                          (FX_curvature_agg['Kb_M'] != FX_curvature_agg['Kb+_M'])],
                                         [(FX_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (FX_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    FX_curvature_agg['Kb+_H']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Up'])))
    FX_curvature_agg['Kb-_H']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Down'])))
    FX_curvature_agg['Kb_H']=np.maximum(FX_curvature_agg['Kb+_H'],FX_curvature_agg['Kb-_H'])
    FX_curvature_agg['Kb_H^2']=np.square(FX_curvature_agg['Kb_H'])
    FX_curvature_agg['Sb_H']=np.select([(FX_curvature_agg['Kb_H'] == FX_curvature_agg['Kb+_H']),
                                          (FX_curvature_agg['Kb_H'] != FX_curvature_agg['Kb+_H'])],
                                         [(FX_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (FX_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    FX_curvature_agg['Kb+_L']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Up'])))
    FX_curvature_agg['Kb-_L']=np.sqrt(np.maximum(0,(FX_curvature_agg['max_0_square/Curvature Down'])))
    FX_curvature_agg['Kb_L']=np.maximum(FX_curvature_agg['Kb+_L'],FX_curvature_agg['Kb-_L'])
    FX_curvature_agg['Kb_L^2']=np.square(FX_curvature_agg['Kb_L'])
    FX_curvature_agg['Sb_L']=np.select([(FX_curvature_agg['Kb_L'] == FX_curvature_agg['Kb+_L']),
                                          (FX_curvature_agg['Kb_L'] != FX_curvature_agg['Kb+_L'])],
                                         [(FX_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Up']),
                                          (FX_curvature_agg['SENSITIVITY_VAL_RPT_CURR_CNY/Curvature Down'])])

    FX_curvature_agg['max']=np.select([(FX_curvature_agg['Kb_M'] == FX_curvature_agg['Kb+_M']),
                                         (FX_curvature_agg['Kb_M'] != FX_curvature_agg['Kb+_M'])],
                                        [(FX_curvature_agg['max_0_k/Curvature Up']),
                                         (FX_curvature_agg['max_0_k/Curvature Down'])])

    FX_curvature_agg['sign']=np.select([(FX_curvature_agg['Kb_M'] == FX_curvature_agg['Kb+_M']),
                                          (FX_curvature_agg['Kb_M'] != FX_curvature_agg['Kb+_M'])],
                                         ['Curvature Up','Curvature Down'])

    FX_curvature_bc=FX_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Sb_M']]
    FX_curvature_bc=FX_curvature_bc.rename(
        {'Sb_M':'Sb','RISK_FACTOR_BUCKET':'Bucket_b'},axis=1
    ).merge(FX_curvature_bc.rename(
        {'Sb_M':'Sc','RISK_FACTOR_BUCKET':'Bucket_c'},axis=1
    ),on=['RISK_FACTOR_CLASS'],how='left')
    #FX_curvature_bc=FX_curvature_bc[(FX_curvature_bc['Bucket_b']!=FX_curvature_bc['Bucket_c'])]

    FX_curvature_bc.loc[(FX_curvature_bc['Sb']<0) & (FX_curvature_bc['Sc']<0),'Psi']=0
    FX_curvature_bc.loc[(FX_curvature_bc['Sb']>=0) | (FX_curvature_bc['Sc']>=0),'Psi']=1
    
    FX_curvature_bc.loc[(FX_curvature_bc['Bucket_b']!=FX_curvature_bc['Bucket_c']),'Gamma_bc']=FX_Gamma
    FX_curvature_bc.loc[(FX_curvature_bc['Bucket_b']==FX_curvature_bc['Bucket_c']),'Gamma_bc']=0
    
    FX_curvature_bc['Gamma_bc_M']=np.square(FX_curvature_bc['Gamma_bc'])

    FX_curvature_bc['rslt_bc_M']=FX_curvature_bc['Gamma_bc_M']*FX_curvature_bc['Psi']*FX_curvature_bc['Sb']*FX_curvature_bc['Sc']
    FX_curvature_bc['Gamma_bc_H']=np.square(np.minimum(1,FX_curvature_bc['Gamma_bc']*High_Multipler))
    FX_curvature_bc['Gamma_bc_L']=np.square(np.maximum((Low_Multipler1*FX_curvature_bc['Gamma_bc']-1),(Low_Multipler2*FX_curvature_bc['Gamma_bc'])))
    FX_curvature_bc['rslt_bc_H']=FX_curvature_bc['Gamma_bc_H']*FX_curvature_bc['Psi']*FX_curvature_bc['Sb']*FX_curvature_bc['Sc']
    FX_curvature_bc['rslt_bc_L']=FX_curvature_bc['Gamma_bc_L']*FX_curvature_bc['Psi']*FX_curvature_bc['Sb']*FX_curvature_bc['Sc']

    FX_curvature_bc['gammac_M']=FX_curvature_bc['Gamma_bc_M']*FX_curvature_bc['Psi']*FX_curvature_bc['Sc']
    FX_curvature_bc['gammac_H']=FX_curvature_bc['Gamma_bc_H']*FX_curvature_bc['Psi']*FX_curvature_bc['Sc']
    FX_curvature_bc['gammac_L']=FX_curvature_bc['Gamma_bc_L']*FX_curvature_bc['Psi']*FX_curvature_bc['Sc']

    fxc_M_est=sum(FX_curvature_agg['Kb_M^2'])+sum(FX_curvature_bc['rslt_bc_M'])
    fxc_H_est=sum(FX_curvature_agg['Kb_H^2'])+sum(FX_curvature_bc['rslt_bc_H'])
    fxc_L_est=sum(FX_curvature_agg['Kb_L^2'])+sum(FX_curvature_bc['rslt_bc_L'])

    fxc_M = np.sqrt(np.maximum(0,sum(FX_curvature_agg['Kb_M^2'])+sum(FX_curvature_bc['rslt_bc_M'])))
    fxc_H = np.sqrt(np.maximum(0,sum(FX_curvature_agg['Kb_H^2'])+sum(FX_curvature_bc['rslt_bc_H'])))
    fxc_L = np.sqrt(np.maximum(0,sum(FX_curvature_agg['Kb_L^2'])+sum(FX_curvature_bc['rslt_bc_L'])))

    fxc=pd.DataFrame([],columns=['RISK_FACTOR_CLASS','SENS_TYPE','NORMAL','HIGH','LOW'],index=[0])

    fxc['RISK_FACTOR_CLASS']='FX'
    fxc['SENS_TYPE']='CURVATURE'
    fxc['NORMAL']=fxc_M
    fxc['HIGH']=fxc_H
    fxc['LOW']=fxc_L

    fxc_1=FX_curvature[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','WEIGHTED_SENSITIVITY']]
    fxc_3=FX_curvature_bc.groupby(['RISK_FACTOR_CLASS','Bucket_b']
                                      ,dropna=False).agg({'gammac_M':'sum','gammac_H':'sum','gammac_L':'sum'}).reset_index()
    fxc_4=FX_curvature_agg[['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET','Kb_M','Kb_H','Kb_L','max','sign']]

    fxc_decomp=fxc_1.merge(fxc_3,left_on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET']
                               ,right_on=['RISK_FACTOR_CLASS','Bucket_b'],how='left')\
    .merge(fxc_4,on=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'],how='left')\
    .merge(fxc,on=['RISK_FACTOR_CLASS'],how='left')

    fxc_decomp=fxc_decomp.drop(['Bucket_b','SENS_TYPE'],axis=1)

    fxc_decomp['M_est']=fxc_M_est
    fxc_decomp['H_est']=fxc_H_est
    fxc_decomp['L_est']=fxc_L_est

    fxc_decomp=fxc_decomp[(fxc_decomp.SENSITIVITY_TYPE==fxc_decomp.sign)]

    #case 1/2
    fxc_decomp.loc[(fxc_decomp['M_est']>=0),'pder_M']=(fxc_decomp['max']+fxc_decomp['gammac_M'])/fxc_decomp['NORMAL']
    fxc_decomp.loc[(fxc_decomp['H_est']>=0),'pder_H']=(fxc_decomp['max']+fxc_decomp['gammac_H'])/fxc_decomp['HIGH']
    fxc_decomp.loc[(fxc_decomp['L_est']>=0),'pder_L']=(fxc_decomp['max']+fxc_decomp['gammac_L'])/fxc_decomp['LOW']

    #case 3 
    fxc_decomp.loc[(fxc_decomp['M_est']<0),'pder_M']=0
    fxc_decomp.loc[(fxc_decomp['H_est']<0),'pder_H']=0
    fxc_decomp.loc[(fxc_decomp['L_est']<0),'pder_L']=0

    fxc_decomp=fxc_decomp[['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE','pder_M','pder_H','pder_L']]

    fxc_decomp_rslt=FX_curvature.merge(fxc_decomp,on=['RISK_FACTOR_CLASS','RISK_FACTOR_ID','RISK_FACTOR_BUCKET','SENSITIVITY_TYPE'],how='right')

    return FX_curvature, FX_curvature_agg, fxc, fxc_decomp_rslt


# #### execute

# In[103]:


def exct(Raw_Data):
    
    df=Raw_Data

    if df[df['RISK_FACTOR_CLASS']=='GIRR'].empty:
        GIRR_delta = pd.DataFrame()
        GIRR_delta_agg = pd.DataFrame()
        girrd = pd.DataFrame()
        girrd_decomp_rslt = pd.DataFrame()
        GIRR_vega = pd.DataFrame()
        GIRR_vega_agg = pd.DataFrame()
        girrv = pd.DataFrame()
        girrv_decomp_rslt = pd.DataFrame()
        GIRR_curvature = pd.DataFrame()
        GIRR_curvature_agg = pd.DataFrame()
        girrc = pd.DataFrame()
        girrc_decomp_rslt = pd.DataFrame()
        pass
    else:
        if df[(df['RISK_FACTOR_CLASS']=='GIRR')&(df['SENSITIVITY_TYPE']=='Delta')].empty:
            GIRR_delta = pd.DataFrame()
            GIRR_delta_agg = pd.DataFrame()
            girrd = pd.DataFrame()
            girrd_decomp_rslt = pd.DataFrame()
        else:
            girrdelta_all = GIRR_Delta(df)
            GIRR_delta = girrdelta_all[0]
            GIRR_delta_agg = girrdelta_all[1]
            girrd = girrdelta_all[2]
            girrd_decomp_rslt = girrdelta_all[3]

        if df[(df['RISK_FACTOR_CLASS']=='GIRR')&(df['SENSITIVITY_TYPE']=='Vega')].empty:
            GIRR_vega = pd.DataFrame()
            GIRR_vega_agg = pd.DataFrame()
            girrv = pd.DataFrame()
            girrv_decomp_rslt = pd.DataFrame()
        else:
            girrvega_all = GIRR_Vega(df)
            GIRR_vega = girrvega_all[0]
            GIRR_vega_agg = girrvega_all[1]
            girrv = girrvega_all[2]
            girrv_decomp_rslt = girrvega_all[3]

        if df.loc[(df['RISK_FACTOR_CLASS']=='GIRR')&((df['SENSITIVITY_TYPE']=='Curvature Up')|(df['SENSITIVITY_TYPE']=='Curvature Down'))].empty:
            GIRR_curvature = pd.DataFrame()
            GIRR_curvature_agg = pd.DataFrame()
            girrc = pd.DataFrame()
            girrc_decomp_rslt = pd.DataFrame()
        else:
            girrcvt_all = GIRR_Curvature(df)
            GIRR_curvature = girrcvt_all[0]
            GIRR_curvature_agg = girrcvt_all[1]
            girrc = girrcvt_all[2]
            girrc_decomp_rslt = girrcvt_all[3]


    if df[df['RISK_FACTOR_CLASS']=='CSR (non-sec)'].empty:
        CSR_delta = pd.DataFrame()
        CSR_delta_agg = pd.DataFrame()
        csrd = pd.DataFrame()
        csrd_decomp_rslt = pd.DataFrame()
        CSR_vega = pd.DataFrame()
        CSR_vega_agg = pd.DataFrame()
        csrv = pd.DataFrame()
        csrv_decomp_rslt = pd.DataFrame()
        CSR_curvature = pd.DataFrame()
        CSR_curvature_agg = pd.DataFrame()
        csrc = pd.DataFrame()
        csrc_decomp_rslt = pd.DataFrame()
        pass
    else:
        if df[(df['RISK_FACTOR_CLASS']=='CSR (non-sec)')&(df['SENSITIVITY_TYPE']=='Delta')].empty:
            CSR_delta = pd.DataFrame()
            CSR_delta_agg = pd.DataFrame()
            csrd = pd.DataFrame()
            csrd_decomp_rslt = pd.DataFrame()
        else:
            csrdelta_all = CSR_Delta(df)
            CSR_delta = csrdelta_all[0]
            CSR_delta_agg = csrdelta_all[1]
            csrd = csrdelta_all[2]
            csrd_decomp_rslt = csrdelta_all[3]

        if df[(df['RISK_FACTOR_CLASS']=='CSR (non-sec)')&(df['SENSITIVITY_TYPE']=='Vega')].empty:
            CSR_vega = pd.DataFrame()
            CSR_vega_agg = pd.DataFrame()
            csrv = pd.DataFrame()
            csrv_decomp_rslt = pd.DataFrame()
        else:
            csrvega_all = CSR_Vega(df)
            CSR_vega = csrvega_all[0]
            CSR_vega_agg = csrvega_all[1]
            csrv = csrvega_all[2]
            csrv_decomp_rslt = csrvega_all[3]

        if df.loc[(df['RISK_FACTOR_CLASS']=='CSR (non-sec)')&((df['SENSITIVITY_TYPE']=='Curvature Up')|(df['SENSITIVITY_TYPE']=='Curvature Down'))].empty:
            CSR_curvature = pd.DataFrame()
            CSR_curvature_agg = pd.DataFrame()
            csrc = pd.DataFrame()
            csrc_decomp_rslt = pd.DataFrame()
        else:
            csrcvt_all = CSR_Curvature(df)
            CSR_curvature = csrcvt_all[0]
            CSR_curvature_agg = csrcvt_all[1]
            csrc = csrcvt_all[2]
            csrc_decomp_rslt = csrcvt_all[3]


    if df[df['RISK_FACTOR_CLASS']=='CSR (non-ctp)'].empty:
        CSRNC_delta = pd.DataFrame()
        CSRNC_delta_agg = pd.DataFrame()
        csrncd = pd.DataFrame()
        csrncd_decomp_rslt = pd.DataFrame()
        CSRNC_vega = pd.DataFrame()
        CSRNC_vega_agg = pd.DataFrame()
        csrncv = pd.DataFrame()
        csrncv_decomp_rslt = pd.DataFrame()
        CSRNC_curvature = pd.DataFrame()
        CSRNC_curvature_agg = pd.DataFrame()
        csrncc = pd.DataFrame()
        csrncc_decomp_rslt = pd.DataFrame()
        pass
    else:
        if df[(df['RISK_FACTOR_CLASS']=='CSR (non-ctp)')&(df['SENSITIVITY_TYPE']=='Delta')].empty:
            CSRNC_delta = pd.DataFrame()
            CSRNC_delta_agg = pd.DataFrame()
            csrncd = pd.DataFrame()
            csrncd_decomp_rslt = pd.DataFrame()
        else:
            csrncdelta_all = CSRNC_Delta(df)
            CSRNC_delta = csrncdelta_all[0]
            CSRNC_delta_agg = csrncdelta_all[1]
            csrncd = csrncdelta_all[2]
            csrncd_decomp_rslt = csrncdelta_all[3]

        if df[(df['RISK_FACTOR_CLASS']=='CSR (non-ctp)')&(df['SENSITIVITY_TYPE']=='Vega')].empty:
            CSRNC_vega = pd.DataFrame()
            CSRNC_vega_agg = pd.DataFrame()
            csrncv = pd.DataFrame()
            csrncv_decomp_rslt = pd.DataFrame()
        else:
            csrncvega_all = CSRNC_Vega(df)
            CSRNC_vega = csrncvega_all[0]
            CSRNC_vega_agg = csrncvega_all[1]
            csrncv = csrncvega_all[2]
            csrncv_decomp_rslt = csrncvega_all[3]

        if df.loc[(df['RISK_FACTOR_CLASS']=='CSR (non-ctp)')&((df['SENSITIVITY_TYPE']=='Curvature Up')|(df['SENSITIVITY_TYPE']=='Curvature Down'))].empty:
            CSRNC_curvature = pd.DataFrame()
            CSRNC_curvature_agg = pd.DataFrame()
            csrncc = pd.DataFrame()
            csrncc_decomp_rslt = pd.DataFrame()
        else:
            csrnccvt_all = CSRNC_Curvature(df)
            CSRNC_curvature = csrnccvt_all[0]
            CSRNC_curvature_agg = csrnccvt_all[1]
            csrncc = csrnccvt_all[2]
            csrncc_decomp_rslt = csrnccvt_all[3]


    if df[df['RISK_FACTOR_CLASS']=='CSR (ctp)'].empty:
        CSRC_delta = pd.DataFrame()
        CSRC_delta_agg = pd.DataFrame()
        csrcd = pd.DataFrame()
        csrcd_decomp_rslt = pd.DataFrame()
        CSRC_vega = pd.DataFrame()
        CSRC_vega_agg = pd.DataFrame()
        csrcv = pd.DataFrame()
        csrcv_decomp_rslt = pd.DataFrame()
        CSRC_curvature = pd.DataFrame()
        CSRC_curvature_agg = pd.DataFrame()
        csrcc = pd.DataFrame()
        csrcc_decomp_rslt = pd.DataFrame()
        pass
    else:
        if df[(df['RISK_FACTOR_CLASS']=='CSR (ctp)')&(df['SENSITIVITY_TYPE']=='Delta')].empty:
            CSRC_delta = pd.DataFrame()
            CSRC_delta_agg = pd.DataFrame()
            csrcd = pd.DataFrame()
            csrcd_decomp_rslt = pd.DataFrame()
        else:
            csrcdelta_all = CSRC_Delta(df)
            CSRC_delta = csrcdelta_all[0]
            CSRC_delta_agg = csrcdelta_all[1]
            csrcd = csrcdelta_all[2]
            csrcd_decomp_rslt = csrcdelta_all[3]

        if df[(df['RISK_FACTOR_CLASS']=='CSR (ctp)')&(df['SENSITIVITY_TYPE']=='Vega')].empty:
            CSRC_vega = pd.DataFrame()
            CSRC_vega_agg = pd.DataFrame()
            csrcv = pd.DataFrame()
            csrcv_decomp_rslt = pd.DataFrame()
        else:
            csrcvega_all = CSRC_Vega(df)
            CSRC_vega = csrcvega_all[0]
            CSRC_vega_agg = csrcvega_all[1]
            csrcv = csrcvega_all[2]
            csrcv_decomp_rslt = csrcvega_all[3]

        if df.loc[(df['RISK_FACTOR_CLASS']=='CSR (ctp)')&((df['SENSITIVITY_TYPE']=='Curvature Up')|(df['SENSITIVITY_TYPE']=='Curvature Down'))].empty:
            CSRC_curvature = pd.DataFrame()
            CSRC_curvature_agg = pd.DataFrame()
            csrcc = pd.DataFrame()
            csrcc_decomp_rslt = pd.DataFrame()
        else:
            csrccvt_all = CSRC_Curvature(df)
            CSRC_curvature = csrccvt_all[0]
            CSRC_curvature_agg = csrccvt_all
            csrcc = csrccvt_all[2]
            csrcc_decomp_rslt = csrccvt_all[3]


    if df[df['RISK_FACTOR_CLASS']=='EQ'].empty:
        EQ_delta = pd.DataFrame()
        EQ_delta_agg = pd.DataFrame()
        eqd = pd.DataFrame()
        eqd_decomp_rslt = pd.DataFrame()
        EQ_vega = pd.DataFrame()
        EQ_vega_agg = pd.DataFrame()
        eqv = pd.DataFrame()
        eqv_decomp_rslt = pd.DataFrame()
        EQ_curvature = pd.DataFrame()
        EQ_curvature_agg = pd.DataFrame()
        eqc = pd.DataFrame()
        eqc_decomp_rslt = pd.DataFrame()
        pass
    else:
        if df[(df['RISK_FACTOR_CLASS']=='EQ')&(df['SENSITIVITY_TYPE']=='Delta')].empty:
            EQ_delta = pd.DataFrame()
            EQ_delta_agg = pd.DataFrame()
            eqd = pd.DataFrame()
            eqd_decomp_rslt = pd.DataFrame()
        else:
            eqdelta_all = EQ_Delta(df)
            EQ_delta = eqdelta_all[0]
            EQ_delta_agg = eqdelta_all[1]
            eqd = eqdelta_all[2]
            eqd_decomp_rslt = eqdelta_all[3]

        if df[(df['RISK_FACTOR_CLASS']=='EQ')&(df['SENSITIVITY_TYPE']=='Vega')].empty:
            EQ_vega = pd.DataFrame()
            EQ_vega_agg = pd.DataFrame()
            eqv = pd.DataFrame()
            eqv_decomp_rslt = pd.DataFrame()
        else:
            eqvega_all = EQ_Vega(df)
            EQ_vega = eqvega_all[0]
            EQ_vega_agg = eqvega_all[1]
            eqv = eqvega_all[2]
            eqv_decomp_rslt = eqvega_all[3]

        if df.loc[(df['RISK_FACTOR_CLASS']=='EQ')&((df['SENSITIVITY_TYPE']=='Curvature Up')|(df['SENSITIVITY_TYPE']=='Curvature Down'))].empty:
            EQ_curvature = pd.DataFrame()
            EQ_curvature_agg = pd.DataFrame()
            eqc = pd.DataFrame()
            eqc_decomp_rslt = pd.DataFrame()
        else:
            eqcvt_all = EQ_Curvature(df)
            EQ_curvature = eqcvt_all[0]
            EQ_curvature_agg = eqcvt_all[1]
            eqc = eqcvt_all[2]
            eqc_decomp_rslt = eqcvt_all[3]


    if df[df['RISK_FACTOR_CLASS']=='CMTY'].empty:
        CMTY_delta = pd.DataFrame()
        CMTY_delta_agg = pd.DataFrame()
        cmtyd = pd.DataFrame()
        cmtyd_decomp_rslt = pd.DataFrame()
        CMTY_vega = pd.DataFrame()
        CMTY_vega_agg = pd.DataFrame()
        cmtyv = pd.DataFrame()
        cmtyv_decomp_rslt = pd.DataFrame()
        CMTY_curvature = pd.DataFrame()
        CMTY_curvature_agg = pd.DataFrame()
        cmtyc = pd.DataFrame()
        cmtyc_decomp_rslt = pd.DataFrame()
        pass
    else:
        if df[(df['RISK_FACTOR_CLASS']=='CMTY')&(df['SENSITIVITY_TYPE']=='Delta')].empty:
            CMTY_delta = pd.DataFrame()
            CMTY_delta_agg = pd.DataFrame()
            cmtyd = pd.DataFrame()
            cmtyd_decomp_rslt = pd.DataFrame()
        else:
            cmtydelta_all = CMTY_Delta(df)
            CMTY_delta = cmtydelta_all[0]
            CMTY_delta_agg = cmtydelta_all[1]
            cmtyd = cmtydelta_all[2]
            cmtyd_decomp_rslt = cmtydelta_all[3]

        if df[(df['RISK_FACTOR_CLASS']=='CMTY')&(df['SENSITIVITY_TYPE']=='Vega')].empty:
            CMTY_vega = pd.DataFrame()
            CMTY_vega_agg = pd.DataFrame()
            cmtyv = pd.DataFrame()
            cmtyv_decomp_rslt = pd.DataFrame()
        else:
            cmtyvega_all = CMTY_Vega(df)
            CMTY_vega = cmtyvega_all[0]
            CMTY_vega_agg = cmtyvega_all[1]
            cmtyv = cmtyvega_all[2]
            cmtyv_decomp_rslt = cmtyvega_all[3]

        if df.loc[(df['RISK_FACTOR_CLASS']=='CMTY')&((df['SENSITIVITY_TYPE']=='Curvature Up')|(df['SENSITIVITY_TYPE']=='Curvature Down'))].empty:
            CMTY_curvature = pd.DataFrame()
            CMTY_curvature_agg = pd.DataFrame()
            cmtyc = pd.DataFrame()
            cmtyc_decomp_rslt = pd.DataFrame()
        else:
            cmtycvt_all = CMTY_Curvature(df)
            CMTY_curvature = cmtycvt_all[0]
            CMTY_curvature_agg = cmtycvt_all[1]
            cmtyc = cmtycvt_all[2]
            cmtyc_decomp_rslt = cmtycvt_all[3]


    if df[df['RISK_FACTOR_CLASS']=='FX'].empty:
        FX_delta = pd.DataFrame()
        FX_delta_agg = pd.DataFrame()
        fxd = pd.DataFrame()
        fxd_decomp_rslt = pd.DataFrame()
        FX_vega = pd.DataFrame()
        FX_vega_agg = pd.DataFrame()
        fxv = pd.DataFrame()
        fxv_decomp_rslt = pd.DataFrame()
        FX_curvature = pd.DataFrame()
        FX_curvature_agg = pd.DataFrame()
        fxc = pd.DataFrame()
        fxc_decomp_rslt = pd.DataFrame()
        pass
    else:
        if df[(df['RISK_FACTOR_CLASS']=='FX')&(df['SENSITIVITY_TYPE']=='Delta')].empty:
            FX_delta = pd.DataFrame()
            FX_delta_agg = pd.DataFrame()
            fxd = pd.DataFrame()
            fxd_decomp_rslt = pd.DataFrame()
        else:
            fxdelta_all = FX_Delta(df)
            FX_delta = fxdelta_all[0]
            FX_delta_agg = fxdelta_all[1]
            fxd = fxdelta_all[2]
            fxd_decomp_rslt = fxdelta_all[3]

        if df.loc[(df['SENSITIVITY_TYPE']=='Vega')&(df['RISK_FACTOR_CLASS']=='FX')].empty:
            FX_vega = pd.DataFrame()
            FX_vega_agg = pd.DataFrame()
            fxv = pd.DataFrame()
            fxv_decomp_rslt = pd.DataFrame()
        else:
            fxvega_all = FX_Vega(df)
            FX_vega = fxvega_all[0]
            FX_vega_agg = fxvega_all[1]
            fxv = fxvega_all[2]
            fxv_decomp_rslt = fxvega_all[3]

        if df.loc[((df['SENSITIVITY_TYPE']=='Curvature Up')|(df['SENSITIVITY_TYPE']=='Curvature Down'))&(df['RISK_FACTOR_CLASS']=='FX')].empty:
            FX_curvature = pd.DataFrame()
            FX_curvature_agg = pd.DataFrame()
            fxc = pd.DataFrame()
            fxc_decomp_rslt = pd.DataFrame()
        else:
            fxcvt_all = FX_Curvature(df)
            FX_curvature = fxcvt_all[0]
            FX_curvature_agg = fxcvt_all[1]
            fxc = fxcvt_all[2]
            fxc_decomp_rslt = fxcvt_all[3]

    pos=pd.concat([GIRR_delta,GIRR_vega,GIRR_curvature
                   ,CSR_delta,CSR_vega,CSR_curvature
                   ,CSRNC_delta,CSRNC_vega,CSRNC_curvature
                   ,CSRC_delta,CSRC_vega,CSRC_curvature
                   ,EQ_delta,EQ_vega,EQ_curvature
                   ,CMTY_delta,CMTY_vega,CMTY_curvature
                   ,FX_delta,FX_vega,FX_curvature]
                  ,join="outer",ignore_index=True)

    pos_col = pd.DataFrame(columns=['RISK_FACTOR_ID', 'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2'
             , 'RISK_FACTOR_CLASS', 'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE'
             , 'SENSITIVITY_TYPE', 'SENSITIVITY_VAL_RPT_CURR_CNY', 'RISKWEIGHT'])
    
    pos=pd.concat([pos_col,pos],join="outer",ignore_index=True)

    pos=pos[['RISK_FACTOR_ID', 'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2'
             , 'RISK_FACTOR_CLASS', 'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE'
             , 'SENSITIVITY_TYPE', 'SENSITIVITY_VAL_RPT_CURR_CNY', 'RISKWEIGHT']]
    
    bucket=pd.concat([GIRR_delta_agg,GIRR_vega_agg,GIRR_curvature_agg
                      ,CSR_delta_agg,CSR_vega_agg,CSR_curvature_agg
                      ,CSRNC_delta_agg,CSRNC_vega_agg,CSRNC_curvature_agg
                      ,CSRC_delta_agg,CSRC_vega_agg,CSRC_curvature_agg
                      ,EQ_delta_agg,EQ_vega_agg,EQ_curvature_agg
                      ,CMTY_delta_agg,CMTY_vega_agg,CMTY_curvature_agg
                      ,FX_delta_agg,FX_vega_agg,FX_curvature_agg]
                     ,join="outer",ignore_index=True)
    
    bucket_col=pd.DataFrame(columns=['RISK_FACTOR_CLASS', 'RISK_FACTOR_BUCKET'
                   , 'Kb+_M', 'Kb-_M', 'Kb_M', 'Kb_M^2', 'Sb_M'
                   , 'Kb+_H', 'Kb-_H', 'Kb_H', 'Kb_H^2', 'Sb_H'
                   , 'Kb+_L', 'Kb-_L', 'Kb_L', 'Kb_L^2', 'Sb_L'
                   , 'Sb*_M', 'Sb*_H', 'Sb*_L'])
    
    bucket = pd.concat([bucket_col,bucket],join="outer",ignore_index=True)

    bucket=bucket[['RISK_FACTOR_CLASS', 'RISK_FACTOR_BUCKET'
                   , 'Kb+_M', 'Kb-_M', 'Kb_M', 'Kb_M^2', 'Sb_M'
                   , 'Kb+_H', 'Kb-_H', 'Kb_H', 'Kb_H^2', 'Sb_H'
                   , 'Kb+_L', 'Kb-_L', 'Kb_L', 'Kb_L^2', 'Sb_L'
                   , 'Sb*_M', 'Sb*_H', 'Sb*_L']]

    class_=pd.concat([girrd,girrv,girrc
                      ,csrd,csrv,csrc
                      ,csrncd,csrncv,csrncc
                      ,csrcd,csrcv,csrcc
                      ,eqd,eqv,eqc
                      ,cmtyd,cmtyv,cmtyc
                      ,fxd,fxv,fxc]
                     ,ignore_index=True)

    class_=class_.pivot(index=('RISK_FACTOR_CLASS')
                         ,columns='SENS_TYPE')

    class_.columns=['_'.join(i) for i in class_.columns]

    class_=class_.reset_index()
    
    class_col = pd.DataFrame([],columns = ['RISK_FACTOR_CLASS'
                                           ,'NORMAL_CURVATURE','NORMAL_DELTA','NORMAL_VEGA'
                                           ,'HIGH_CURVATURE','HIGH_DELTA','HIGH_VEGA'
                                           ,'LOW_CURVATURE','LOW_DELTA','LOW_VEGA'])
    class_ = pd.concat([class_col,class_],ignore_index=True).reset_index(drop=True)

    class_.loc[:,'NORMAL'] = class_.loc[:,['NORMAL_DELTA','NORMAL_VEGA','NORMAL_CURVATURE']].sum(axis=1)
    class_.loc[:,'HIGH'] = class_.loc[:,['HIGH_DELTA','HIGH_VEGA','HIGH_CURVATURE']].sum(axis=1)
    class_.loc[:,'LOW'] = class_.loc[:,['LOW_DELTA','LOW_VEGA','LOW_CURVATURE']].sum(axis=1)

    class_.loc[:,'RISK_CHARGE']=class_.loc[:,['NORMAL','HIGH','LOW']].max(axis=1)

    class_['MAX_SIGN']=class_[['NORMAL', 'HIGH', 'LOW']].idxmax(1)

    class_.loc[:,'GROUP_TYPE']=np.nan
    class_.loc[:,'GROUP_VALUE']=np.nan

    decomp_rslt=pd.concat([girrd_decomp_rslt,girrv_decomp_rslt,girrc_decomp_rslt
                           ,csrd_decomp_rslt,csrv_decomp_rslt,csrc_decomp_rslt
                           ,csrncd_decomp_rslt,csrncv_decomp_rslt,csrncc_decomp_rslt
                           ,csrcd_decomp_rslt,csrcv_decomp_rslt,csrcc_decomp_rslt
                           ,eqd_decomp_rslt,eqv_decomp_rslt,eqc_decomp_rslt
                           ,cmtyd_decomp_rslt,cmtyv_decomp_rslt,cmtyc_decomp_rslt
                           ,fxd_decomp_rslt,fxv_decomp_rslt,fxc_decomp_rslt]
                          ,ignore_index=True)

    decomp_rslt=decomp_rslt.merge(class_[['RISK_FACTOR_CLASS','MAX_SIGN']],on=['RISK_FACTOR_CLASS'],how='left')

    decomp_rslt.loc[decomp_rslt.MAX_SIGN=='NORMAL','PARTIAL_DERIVATIVE']=decomp_rslt.pder_M
    decomp_rslt.loc[decomp_rslt.MAX_SIGN=='HIGH','PARTIAL_DERIVATIVE']=decomp_rslt.pder_H
    decomp_rslt.loc[decomp_rslt.MAX_SIGN=='LOW','PARTIAL_DERIVATIVE']=decomp_rslt.pder_L

    decomp_rslt['CONTRIBUTION']=decomp_rslt.PARTIAL_DERIVATIVE*decomp_rslt.WEIGHTED_SENSITIVITY

    decomp_rslt_col = pd.DataFrame([],columns=['RISK_FACTOR_ID',
           'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2', 'RISK_FACTOR_CLASS',
           'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE', 'SENSITIVITY_TYPE', 
           'WEIGHTED_SENSITIVITY', 'CONTRIBUTION'])
    decomp_rslt = pd.concat([decomp_rslt,decomp_rslt_col],ignore_index=True).reset_index(drop=True)
    
    decomp_rslt=decomp_rslt[['RISK_FACTOR_ID',
           'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2', 'RISK_FACTOR_CLASS',
           'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE', 'SENSITIVITY_TYPE', 
           'WEIGHTED_SENSITIVITY', 'CONTRIBUTION']]

    decomp_rslt.loc[decomp_rslt.SENSITIVITY_TYPE=='Curvature Up','SENSITIVITY_TYPE']='Curvature'
    decomp_rslt.loc[decomp_rslt.SENSITIVITY_TYPE=='Curvature Down','SENSITIVITY_TYPE']='Curvature'

    riskfactor=decomp_rslt.groupby(['RISK_FACTOR_ID',
           'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2', 'RISK_FACTOR_CLASS',
           'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE', 'SENSITIVITY_TYPE'],dropna=False
                                  ).agg({'WEIGHTED_SENSITIVITY':'sum', 'CONTRIBUTION':'sum'}).reset_index()

    riskfactor=riskfactor[['RISK_FACTOR_ID', 'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2',
           'RISK_FACTOR_CLASS', 'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE',
           'SENSITIVITY_TYPE', 'WEIGHTED_SENSITIVITY', 'CONTRIBUTION']]

    level3=decomp_rslt.groupby(['RISK_FACTOR_CLASS','SENSITIVITY_TYPE'
                                , 'RISK_FACTOR_ID', 'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2'
                                , 'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE'],dropna=False
                              ).agg({'WEIGHTED_SENSITIVITY':'sum','CONTRIBUTION':'sum'}).reset_index()

    level3.loc[:,'GROUP_VALUE']=np.nan
    level3.loc[:,'GROUP_TYPE']=np.nan

    level3=level3[['GROUP_TYPE', 'GROUP_VALUE', 'RISK_FACTOR_CLASS', 'SENSITIVITY_TYPE',
           'RISK_FACTOR_ID', 'RISK_FACTOR_VERTEX_1', 'RISK_FACTOR_VERTEX_2',
           'RISK_FACTOR_BUCKET', 'RISK_FACTOR_TYPE', 'WEIGHTED_SENSITIVITY',
           'CONTRIBUTION']]

    class_=class_[['GROUP_TYPE','GROUP_VALUE','RISK_FACTOR_CLASS'
                   , 'HIGH_DELTA', 'NORMAL_DELTA', 'LOW_DELTA'
                   , 'HIGH_VEGA', 'NORMAL_VEGA', 'LOW_VEGA'
                   , 'HIGH_CURVATURE', 'NORMAL_CURVATURE', 'LOW_CURVATURE'
                   , 'RISK_CHARGE']]
    class_.columns=['GROUP_TYPE','GROUP_VALUE','RISK_FACTOR_CLASS'
                   , 'DELTA_HIGH', 'DELTA_NORMAL', 'DELTA_LOW'
                   , 'VEGA_HIGH', 'VEGA_NORMAL', 'VEGA_LOW'
                   , 'CURVATURE_HIGH', 'CURVATURE_NORMAL', 'CURVATURE_LOW'
                   , 'SBA_RISK_CHARGE']

    #class_.columns=pd.DataFrame(list(class_.columns)).loc[:,0]

    bucket.columns=['RISK_FACTOR_CLASS','RISK_FACTOR_BUCKET'
                    ,'KB_P_M','KB_M_M','KB_M','KB2_M','SB_M'
                    ,'KB_P_H','KB_M_H','KB_H','KB2_H','SB_H'
                    ,'KB_P_L','KB_M_L','KB_L','KB2_L','SB_L'
                    ,'SB_STAR_M','SB_STAR_H','SB_STAR_L']

    
    
    return pos, bucket, class_, riskfactor, level3


