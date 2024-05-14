import requests
import zipfile
import io
import pandas as pd
from datetime import date
import datetime
import numpy as np
def download_extract_zip(url, headers):
    response = requests.get(url, headers=headers)
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
                yield zipinfo.filename, thefile
headers = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'referer': 'https://www.nseindia.com/api/chart-databyindex?index=OPTIDXBANKNIFTY25-01-2024CE46000.00',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
    }

def read_bhavcopy_data(data):
  data=data.rename(columns=lambda x:x.strip())
  data['TIMESTAMP']=[datetime.datetime.strptime(i, "%d-%b-%Y").date() for i in data['TIMESTAMP']]
  data['EXPIRY_DT']=[datetime.datetime.strptime(i, "%d-%b-%Y").date() for i in data['EXPIRY_DT']]
  #data['Underlying Value']=[float(list(data['Underlying Value'])[i]) if list(data['Underlying Value'])[i]!='-' else np.nan for i in range(0,len(data))]
  data=data.sort_values(by=['EXPIRY_DT','TIMESTAMP'],ignore_index=True)
  data=data.drop(['Unnamed: 15'],axis=1)
  return data

def extract_bhavcopy(reqdate):
  dd=reqdate.day
  mm=reqdate.strftime("%B")[:3].upper()
  yy=str(reqdate.year)

  url = "https://nsearchives.nseindia.com/content/historical/DERIVATIVES/"+yy+"/"+mm+"/fo"+f"{dd:02d}"+mm+yy+"bhav.csv.zip"

  for filename, file in download_extract_zip(url, headers):
      if filename.endswith('.csv'):
          df = pd.read_csv(file)
  df=read_bhavcopy_data(df)
  return df

reqdata=extract_bhavcopy(date(2024,5,13))
st.dataframe(reqdata.head())
