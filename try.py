from datetime import date,timedelta
import requests
import zipfile
import io
import pandas as pd
from datetime import date
import datetime
import numpy as np
import streamlit as st

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
      'cookie':'defaultLang=en; _ga=GA1.1.1104812566.1715673932; nsit=Vlnxr4g4V-kUaBWzVAIHQWgk; bm_mi=FB4C79C77D0B706493A6766A198D06CB~YAAQUGDQFzhVIXaPAQAAV4XGdhfyD5NWhbBv0peVMKuqn3WGtKr6AzvAt2YU8hH0cXzcGoaZ/GP8ldYHyJeIgbLG02QVhyJqneUSL+zKzUKtDUMZUlPPuKrGL/z6icUE5vsxpFBstu0SeWRMHVgVcY+U1IBOPnrCcIBt9cY6tg9btsIsCFgmaEiY3QBtVh1clBTYl+JOKmr8WxgHBK7HHrzq8dc4JVCcGd49X+VEjbLjfLphoOmcqmwFgc7X8xJNN2GyEGmHLl5qwIAv6SwfhezrIR0XcIRV8MQ37dIjmQAaHF08LCmT6hSfgdI9SDKVc+xW2T7F9uxF/iDIKYQqonSaZml1q7lbvaqz~1; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTcxNTY4NDY4MSwiZXhwIjoxNzE1NjkxODgxfQ.odcaxGBEW_YdBLhWya-HoHVY_bwoGeuilfM7anUm0qY; ak_bmsc=A1EDB9BA40507747ECDEE1E644CCB085~000000000000000000000000000000~YAAQUGDQFxhzIXaPAQAA47bGdhdAc2WLPChoKYYCkSeRGjNtqN1VCuyifKYJtxqgcPK1db5FQzXg9i6nGK7QJke5n96ugK2bYo8tuT2aF1Po0apTf8uzFWDLZ9PyLzrUFpj1Yf6YJUtq8mnTKZgXIxWU52E935OeuDySZ0KLUpnzff7cI06f7Uajjyx6C5rsncEF1CeH3Z52S9W65MbDXYCLrqqx+oELQBA0g49U5/A43L1YyGZgpUu3moCVWlMSIv/TS7vOsPSON5b4+s4aIjQRIogp/7bUlmgCfPLmKRRnJVvj80NON2IabOLq/WJpys1sNQb+7lNtf14v226BLpyKrjoVCYCsjreqZjBsMNmKRWv3HEMN+4Gj2pwdRL57IQbVtKLj6HebHAluJGKKTNeDPspcxH0HxLAZu3m4wHoa6UDlqQYfqB4kDyynPqH+xdqYbrEIR+mcbP83weMkRcbiqSptSAXt2SV7PetKxHz10Xed5Oe6xMSRTphnVpW0zMD9p2faD7TngeSl2+aywe1Z; _ga_QJZ4447QD3=GS1.1.1715684684.2.0.1715684691.0.0.0; _ga_87M7PJ3R97=GS1.1.1715684666.3.1.1715684691.0.0.0; bm_sz=4883DF84D1FDFAB415347CEE711E0D88~YAAQXqg4F5XwzW+PAQAA8LH3dhcyIloeXW2ZbsRK4uPBu6WswMayKDUwXi4GuMRWui3mrbnVYRtbxYx4AKsQ7K1wVHIXUylgjGPpAAgtTMdzDFZm4YnM9pWYfsNqIHAUkyGJxV55XIsRxAe9bYmYlW9k1/p/CQaYgxG/+ZHxyNyOD7c95pDdLei5b5vKXF93j0zvk62LUVsEKu4jx/j3z/43LAijbs4Aj5Vu3AQA4zI9ziaeQmTLZ18kEJl4E3eCSi51vGRLBKrZA3yf+CHUaKWi83Q04XfIiHlb8wSOaXghkRdYf0fXDeFQfw4OEnXA5FjtkYrqM+LLqkzDzUixGwHs+MoqJ3yRnGuIA1je9SWeRgBs0bnWemW4Re6RsByH6KlPso3FNmVOUDF2aUHT1QIvSWJsGxAEc4r0nXly76B0NTe6byBGbi/kuVYelClQ9j17~3420227~3360325; RT="z=1&dm=nseindia.com&si=a746dd1a-fbf2-473e-a100-db75208eb944&ss=lw6ad0s6&sl=0&se=8c&tt=0&bcn=%2F%2F17de4c19.akstat.io%2F"; AKA_A2=A; bm_sv=28B133FEA201D1E9E1B72399747E173C~YAAQUGDQF9CGJ3aPAQAAuzALdxdEiQl8/ixnbF2roWGeZ1dnS2uQ1P0B3h/pCIs5cj/8Cmaek4Ss1ztK9Y/Q5VdM1WtN5Lt3J34SRL5tu2+FSu8K0x4QYHcLA3XnXgfGkSHAhYqyJhKSD952DWCVdCFnflSmF+X6sCEup9iuZz5nf0HhwEg/bG2SDtYP+n3jVLYtCiPzqnN88K+YmpM+BEPPWVBSRsZJ/HYIkZ3p7V3Jd15o7R5OBV4Egh5HZ2rEwplx~1; _abck=AC0A6E579954CFC1BA4A76B56A1628F3~0~YAAQUGDQF+yGJ3aPAQAA6TELdwvlD0hXcTrhAgTSBZM/GKyYE8ZK0AtzWGn1QlSRirkyJ9f3+6qcI5sG7XfU2ocE/gZ/6dtG15qf3SjRv+C/rmRLcG6elqjF5Cko7hfEUN047lh55Bz4eGHbTmrLHTO+F64rVPsrLKTwbLkO9gshpoYcEtQv4pmtPEwXWXUorm2+rHIQofmQYEy7PB68PViAThk6yXgrwYenfYGoeO0MM0Bo663rw83h+iKnRE3yrXFy2rLSEKhj3Zmf3SFjgKyaFOhcJYYhy8AhtHvo7xBETeLpquEUvnMcrsp928Nu4UoICz5hRBcu+nzEhRIPSrG9+6ObNJj500Vi2CrbWWVM5QLu1LPy40Mpc1Nbgms+~-1~-1~-1'
    }
  #st.write(url)
  response = pd.DataFrame(requests.get(url, headers=headers).json()['data'])
  #jobj=response.json()
  #reqdata=pd.DataFrame(jobj['data'])
  reqdata=response.drop(['_id','TIMESTAMP'],axis=1)
  reqdata=reqdata.rename(columns=lambda x:x[3:])
  reqdata['TIMESTAMP']=[datetime.datetime.strptime(i, "%d-%b-%Y").date() for i in reqdata['TIMESTAMP']]
  reqdata['EXPIRY_DT']=[datetime.datetime.strptime(i, "%d-%b-%Y").date() for i in reqdata['EXPIRY_DT']]
  reqdata=reqdata.sort_values(by=['EXPIRY_DT','TIMESTAMP'],ignore_index=True)
  st.write(reqdata)

today= date.today()
start_date=today-timedelta(10)
shares = pd.read_csv("FNO INDICES.csv")
share_list = list(shares["SYMBOL"])
selected_option = st.selectbox("Share List", share_list)
extract_monthly_futidx_data(start_date,today,selected_option)
