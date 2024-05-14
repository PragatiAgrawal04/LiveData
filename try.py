from datetime import date,timedelta

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
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
      'cookie':'nsit=uam-kDW6ljc5yKPv0YPkRBvO; AKA_A2=A; defaultLang=en; ak_bmsc=7ECF063FAD4DA78544BF5051FC22E661~000000000000000000000000000000~YAAQUGDQF5LP33WPAQAAIZkidhdBDTHcIIhLbq6jtrcaBkCqA+R8ReWuGUsobD0fc2Yyg65LPTiu7pqbKqsrYKOXBqhjtyoecek8JrUO+DbX6CnEgL4b+d26m9TnPzPrbXdDovWdigfARp1ed0WpDiKmpRbZn4MRM2owbPf8m5olH5uz8MRh14R6CwfsZhOLUtx1UIJ8d0lck02G2xI9ZujE6LY2XruNMWR1WGrVQ5wVsiUXBPQvm8qvfETthd5l91I87N1wQSbPnhsd1mOyZISjY2lbbcfdWZ71e+Atv+0FaxVopiUXnpNVbIiJpUpw1/1IU4SSCS3U9C13oTbD5QBvkQel+wdlAfUnF3at+Xias3MCMXA+3uQXA/sUYurbkfRlDtPhzcefzLOpBsXK1ZeDIw/p0dKaLLKc70PRHugOAsgKWAVGmQKAa8hkAwfgCA5wr4CMWTZj4Qf4tv0=; _ga=GA1.1.1104812566.1715673932; _abck=AC0A6E579954CFC1BA4A76B56A1628F3~0~YAAQUGDQFzDQ33WPAQAAQqEidgtm1eJ4frtHW8p3lk6094ib064/1DThAkSyv7QB6n/yD3WCdiBcAiAmvD277M6++TntxCn4E/4pP07AxfvtdOLVV0Wp9DUKXwVg2PSe3lwZwif4u8hL9uTyijePpoZKv/1lh63W1vR7SOnEZu1yF2HciKnE5QdMbCjoao8s8WOWm1rfTOwq9kOpayK/CQBVbobh7Ma27R0Qcn6aUdrrrjMfJt4JUqSgm8yCGKuv1JAtYy/kn8UdTGQualja4wBIumlfH8LNPJ4zwu+6wvQka9Ru3/d3n8Vhuustab6mR65k+ZiMw/4uVEeRqV8u6PgHqTqni1QuLYFqvZqLf3f5bVLkGofbuiCBfjQmQOH/AmGErnEIAEo1QdRlzcb4SpyO2Sx6lfeBfd8=~-1~||-1||~-1; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTcxNTY3NDY1MSwiZXhwIjoxNzE1NjgxODUxfQ.SRLnEiu9ssTR8mIiVillu5KM0FxbufsLq4PAFweFiXs; bm_sz=774E3CF122320BA8160DFCD865607056~YAAQUGDQFwvf5HWPAQAARpwtdhdukrSIL1IQEcvLTfgahw5fEB9WtlfiQqNuLwENB+wAiwWnxWsnRsy2YaAaidzmzn47RMKNY1X3udm6b/9VLn2By+o5YJ+Y2qErfJ5+6J70N1YXraYjnd+ZTOyw8EIzEIY/WdgO/YBVjIIrRkfChb3cVv1Z/ONu/CWb21em91ltDwWO6scegzkzP5sdF+Q04ryrbaBdtMa4aVYimV2FNqD4TY4RymtjQ6DeLRx4oVcNiQpR1ayjrWQkqS2oa0Hur+N3QkC7q/gKTEm2PHynwSErsbXq1j3f6+KL1D5ORvkOb7bgsgVoiBABwIFQVQKt6ZYZaBkVKqUmh7eNVb/KSl0a9k/8i53oDDRE/Bo7KPYPCzqRfb6g7n8skjWr3YdszkxFC1VFba4d~3682865~4538931; _ga_QJZ4447QD3=GS1.1.1715674646.1.0.1715674653.0.0.0; _ga_87M7PJ3R97=GS1.1.1715673931.1.1.1715674653.0.0.0; RT="z=1&dm=nseindia.com&si=a746dd1a-fbf2-473e-a100-db75208eb944&ss=lw64dud6&sl=2&se=8c&tt=8mc&bcn=%2F%2F17de4c0d.akstat.io%2F&ld=cn3"; bm_sv=48D3D73A71703246A90AEED6FB84C71F~YAAQUGDQF4b45HWPAQAAItAtdhfZzbzDQ1SigxJdGDRgv0e45/kxMGOoEhMlU47CXYhaIr/tKKCeUSte5Hbp28OxA8qTxpPMU5Wr7grVlydbFUZcx9n7uXjB55O4iaP1jSMt3MGl8/4KYKqnZoaRlfLouURFAUacpP1v4zMLuzwGCbe3lqVGS5JF38PpoygfsZ5TfSfHbJm92CMOGTWpC1t5/kTXmg2NOeZErJIQdnl7vZCCnbYXb/8ryv8c8+8qiKIY~1'
    }
  response = requests.get(url, headers=headers)
  jobj=response.json()
  reqdata=pd.DataFrame(jobj['data'])
  reqdata=reqdata.drop(['_id','TIMESTAMP'],axis=1)
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
