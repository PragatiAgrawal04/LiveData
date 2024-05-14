from datetime import date,timedelta
import requests
import zipfile
import io
import pandas as pd
from datetime import date
import datetime
import numpy as np
def nifty_cash(date,symbol):
  data=yf.download(symbol,start=date,end=date+timedelta(1),interval='5m')
  data=pd.DataFrame(data)
  #data['DateTime']=data.index
  data['Date']=[i.date() for i in data.index]
  data['Time']=[i.time() for i in data.index]
  #data=data.drop(['DateTime','Volume'],axis=1)
  '''change_in_close=[np.round(data.iloc[0,3]-actfutdata.iloc[-1,2],2)]
  for i in range(1,len(data)):
    change_in_close.append(np.round(data.iloc[i,3]-data.iloc[i-1,3],2))
  data['Change_in_close']=change_in_close
  data=data.reset_index(drop=True)'''
  data=data[['Date','Time','Open','High','Low','Close']].reset_index(drop=True)
  return data

def action_setting(futdata):
  exp=futdata['EXPIRY_DT'].unique()
  fdata=pd.DataFrame()
  for i in exp:
    edata=futdata[futdata['EXPIRY_DT']==i].reset_index(drop=True)
    cat,act,spchange,oichange=[str(np.nan)],[np.nan],[np.nan],[np.nan]
    for j in range(1,len(edata)):
      asp,bsp=edata['SETTLE_PRICE'][j-1],edata['SETTLE_PRICE'][j]
      spchange.append(np.round((((bsp)/(asp))-1)*100,2))
      oichange.append(np.round((edata['CHANGE_IN_OI'][j]/edata['OPEN_INT'][j])*100,2))
      sp=[0 if spchange[j]<0 else 1]
      oi=[0 if oichange[j]<0 else 1]
      if sp==[1] and oi==[1]:
        cat.append("LB")
        act.append("Buy")
      elif sp==[0] and oi==[1]:
        cat.append("SB")
        act.append("Sell")
      elif sp==[1] and oi==[0]:
        cat.append("SC")
        act.append("Buy")
      elif sp==[0] and oi==[0]:
        cat.append("LL")
        act.append("Sell")
    edata['Settle Price Change']=spchange
    edata['Movement of OI']=oichange
    edata['Category']=cat
    edata['Action']=act
    fdata=pd.concat([fdata,edata],axis=0)
  fdata=fdata.set_index([pd.Index(range(0,len(fdata)))])
  return fdata

def extract_monthly_futidx_data(start_date,finish_date,symbol):
  sdate=str(start_date.strftime("%d-%m-%Y"))
  edate=str(finish_date.strftime("%d-%m-%Y"))

  url = "https://www.nseindia.com/api/historical/foCPV?from="+sdate+"&to="+edate+"&instrumentType=FUTIDX&symbol="+symbol
  headers = {
      'accept': 'application/json, text/javascript, */*; q=0.01',
      'accept-encoding': 'gzip, deflate, br',
      'accept-language': 'en-US,en;q=0.9',
      'referer': 'https://www.nseindia.com/report-detail/fo_eq_security',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }
  response = requests.get(url, headers=headers).json()['records']['data']
  #jobj=response.json()
  #reqdata=pd.DataFrame(jobj['data'])
  reqdata=response.drop(['_id','TIMESTAMP'],axis=1)
  reqdata=reqdata.rename(columns=lambda x:x[3:])
  reqdata['TIMESTAMP']=[datetime.datetime.strptime(i, "%d-%b-%Y").date() for i in reqdata['TIMESTAMP']]
  reqdata['EXPIRY_DT']=[datetime.datetime.strptime(i, "%d-%b-%Y").date() for i in reqdata['EXPIRY_DT']]
  reqdata=reqdata.sort_values(by=['EXPIRY_DT','TIMESTAMP'],ignore_index=True)
  return reqdata

def execute(date1, date2, sym):
  reqdata=extract_monthly_futidx_data(date1,date2,sym)
  futdata=reqdata.drop(['INSTRUMENT','OPTION_TYPE','MARKET_LOT','STRIKE_PRICE','MARKET_TYPE','TOT_TRADED_QTY','TOT_TRADED_VAL'],axis=1)
  futdata=futdata[['SYMBOL','TIMESTAMP','UNDERLYING_VALUE','EXPIRY_DT','CLOSING_PRICE','PREV_CLS','LAST_TRADED_PRICE','OPENING_PRICE','TRADE_HIGH_PRICE','TRADE_LOW_PRICE','SETTLE_PRICE','OPEN_INT','CHANGE_IN_OI']]
  futdata.iloc[:,4:]=futdata.iloc[:,4:].astype(float)
  actfutdata=action_setting(futdata)
  st.dataframe(actfutdata)
execute(date(2024,5,6),date(2024,5,13),"NIFTY")
