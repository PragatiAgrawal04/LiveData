from datetime import date,timedelta
import requests
import zipfile
import io
import pandas as pd
from datetime import date
import datetime
import numpy as np

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
      'cookie':'defaultLang=en; _ga=GA1.1.1104812566.1715673932; _abck=AC0A6E579954CFC1BA4A76B56A1628F3~0~YAAQUGDQFzDQ33WPAQAAQqEidgtm1eJ4frtHW8p3lk6094ib064/1DThAkSyv7QB6n/yD3WCdiBcAiAmvD277M6++TntxCn4E/4pP07AxfvtdOLVV0Wp9DUKXwVg2PSe3lwZwif4u8hL9uTyijePpoZKv/1lh63W1vR7SOnEZu1yF2HciKnE5QdMbCjoao8s8WOWm1rfTOwq9kOpayK/CQBVbobh7Ma27R0Qcn6aUdrrrjMfJt4JUqSgm8yCGKuv1JAtYy/kn8UdTGQualja4wBIumlfH8LNPJ4zwu+6wvQka9Ru3/d3n8Vhuustab6mR65k+ZiMw/4uVEeRqV8u6PgHqTqni1QuLYFqvZqLf3f5bVLkGofbuiCBfjQmQOH/AmGErnEIAEo1QdRlzcb4SpyO2Sx6lfeBfd8=~-1~||-1||~-1; bm_sz=774E3CF122320BA8160DFCD865607056~YAAQUGDQFwvf5HWPAQAARpwtdhdukrSIL1IQEcvLTfgahw5fEB9WtlfiQqNuLwENB+wAiwWnxWsnRsy2YaAaidzmzn47RMKNY1X3udm6b/9VLn2By+o5YJ+Y2qErfJ5+6J70N1YXraYjnd+ZTOyw8EIzEIY/WdgO/YBVjIIrRkfChb3cVv1Z/ONu/CWb21em91ltDwWO6scegzkzP5sdF+Q04ryrbaBdtMa4aVYimV2FNqD4TY4RymtjQ6DeLRx4oVcNiQpR1ayjrWQkqS2oa0Hur+N3QkC7q/gKTEm2PHynwSErsbXq1j3f6+KL1D5ORvkOb7bgsgVoiBABwIFQVQKt6ZYZaBkVKqUmh7eNVb/KSl0a9k/8i53oDDRE/Bo7KPYPCzqRfb6g7n8skjWr3YdszkxFC1VFba4d~3682865~4538931; nsit=Vlnxr4g4V-kUaBWzVAIHQWgk; bm_mi=FB4C79C77D0B706493A6766A198D06CB~YAAQUGDQFzhVIXaPAQAAV4XGdhfyD5NWhbBv0peVMKuqn3WGtKr6AzvAt2YU8hH0cXzcGoaZ/GP8ldYHyJeIgbLG02QVhyJqneUSL+zKzUKtDUMZUlPPuKrGL/z6icUE5vsxpFBstu0SeWRMHVgVcY+U1IBOPnrCcIBt9cY6tg9btsIsCFgmaEiY3QBtVh1clBTYl+JOKmr8WxgHBK7HHrzq8dc4JVCcGd49X+VEjbLjfLphoOmcqmwFgc7X8xJNN2GyEGmHLl5qwIAv6SwfhezrIR0XcIRV8MQ37dIjmQAaHF08LCmT6hSfgdI9SDKVc+xW2T7F9uxF/iDIKYQqonSaZml1q7lbvaqz~1; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTcxNTY4NDY4MSwiZXhwIjoxNzE1NjkxODgxfQ.odcaxGBEW_YdBLhWya-HoHVY_bwoGeuilfM7anUm0qY; AKA_A2=A; ak_bmsc=A1EDB9BA40507747ECDEE1E644CCB085~000000000000000000000000000000~YAAQUGDQFxhzIXaPAQAA47bGdhdAc2WLPChoKYYCkSeRGjNtqN1VCuyifKYJtxqgcPK1db5FQzXg9i6nGK7QJke5n96ugK2bYo8tuT2aF1Po0apTf8uzFWDLZ9PyLzrUFpj1Yf6YJUtq8mnTKZgXIxWU52E935OeuDySZ0KLUpnzff7cI06f7Uajjyx6C5rsncEF1CeH3Z52S9W65MbDXYCLrqqx+oELQBA0g49U5/A43L1YyGZgpUu3moCVWlMSIv/TS7vOsPSON5b4+s4aIjQRIogp/7bUlmgCfPLmKRRnJVvj80NON2IabOLq/WJpys1sNQb+7lNtf14v226BLpyKrjoVCYCsjreqZjBsMNmKRWv3HEMN+4Gj2pwdRL57IQbVtKLj6HebHAluJGKKTNeDPspcxH0HxLAZu3m4wHoa6UDlqQYfqB4kDyynPqH+xdqYbrEIR+mcbP83weMkRcbiqSptSAXt2SV7PetKxHz10Xed5Oe6xMSRTphnVpW0zMD9p2faD7TngeSl2+aywe1Z; bm_sv=28B133FEA201D1E9E1B72399747E173C~YAAQUGDQF7p2IXaPAQAA2LzGdhd23c8TPjtQBgqFBFe6BvA3uV/t7zePs7QSoQMfhsCVNDfag9Grdlw5AXu9GTvdf99yRsayzV+4b9lvmjF4g8P0U2W5zREgF+j3vaO5Zul2Hi6BCaaKYGojFpl47sVZZJf8yo9FbkhL4ect9/QnVTByALzJbPJwJAjTwpCl6Mrf+Unk7T1KF3WUXEamSlOG28NiuEtO0tAun0z4EZ54d/g8EbvbWgnNIPr8ADCIVf0=~1; RT="z=1&dm=nseindia.com&si=a746dd1a-fbf2-473e-a100-db75208eb944&ss=lw6acm5y&sl=1&se=8c&tt=33w&bcn=%2F%2F17de4c1c.akstat.io%2F&ld=i9c&nu=kpaxjfo&cl=mbs"; _ga_QJZ4447QD3=GS1.1.1715684684.2.0.1715684691.0.0.0; _ga_87M7PJ3R97=GS1.1.1715684666.3.1.1715684691.0.0.0'
    }
  response = pd.DataFrame(requests.get(url, headers=headers).json()['data'])
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
  #futdata=reqdata.drop(['INSTRUMENT','OPTION_TYPE','MARKET_LOT','STRIKE_PRICE','MARKET_TYPE','TOT_TRADED_QTY','TOT_TRADED_VAL'],axis=1)
  #futdata=futdata[['SYMBOL','TIMESTAMP','UNDERLYING_VALUE','EXPIRY_DT','CLOSING_PRICE','PREV_CLS','LAST_TRADED_PRICE','OPENING_PRICE','TRADE_HIGH_PRICE','TRADE_LOW_PRICE','SETTLE_PRICE','OPEN_INT','CHANGE_IN_OI']]
  #futdata.iloc[:,4:]=futdata.iloc[:,4:].astype(float)
  #actfutdata=action_setting(futdata)
  st.dataframe(reqdata)
execute(date(2024,5,6),date(2024,5,13),"NIFTY")
