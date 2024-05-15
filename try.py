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
    'authority':'ww.nseindia.com',
    'method':'GET',
    'path':"/api/historical/foCPV?from="+sdate+"&to="+edate+"&instrumentType=FUTIDX&symbol="+symbol,
    'scheme':'ttps',
    "Accept":"*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Priority": "u=1, i",
    "Referer": "https://www.nseindia.com/report-detail/fo_eq_security",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile":"?0",
    "Sec-Ch-Ua-Platform": "Windows",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    'cookie':'defaultLang=en; _ga=GA1.1.1104812566.1715673932; nsit=fBZAsRmS6SBzo3I8zY3hT15T; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTcxNTc0NzYzMiwiZXhwIjoxNzE1NzU0ODMyfQ.tJRNElVgDeFqSNTZM8IVjPWTXAmbxMJZw2qRDolK13w; bm_mi=9EEEFA2CA5E763BF00FD0D41DA5E14F2~YAAQUGDQF/1cJ3mPAQAA8TSHeheRfTVAdaf0z2sohlUSBoUvVFV9v234XkNNIwpMpU8i4hFl34v6SbmzkPCxFK0pbTNv/Xht6r55TMZ5oVuygvJb4grf3Ei+3zyuHhiVuCFcLXNQkwJQFMXh9DzX2Ne85wfQWBfw6iWB/LiITHRNWfAIAvft9hqqjfWuqitaBAMzFYQh6sIWOe4P66FA7xY0182h+xkrlKlUb0h2P+EJWPdS19mcDwMrW6LTA/Q0MkiCGFKKSIf6HIJmawHT6KpJH/OQpdjCQ4BTgfOkSgnxpnIbCMngJoJR5AJBaqjJWc/w5iq5L3L1fTCa36dydU0Ehfspnflr/5N3~1; bm_sz=D8F50D26DF68B81442CF7EAF9588115D~YAAQUGDQF/9cJ3mPAQAA8TSHehdSSUB4/HftQvklrTWvcn790QMAgORLGJpnwZWhESDqVP0h0BPPXiiQ2FwDg2XCMmXHlaHqyVxdHp/DYfnai47tFCXEGuBezz9f2E05SlzF/zFFMmxfeOBnS8t+uoo5arC9vwa/A4ZwjPWeTNxxtep/u3FgkhHYym16eeNXjS3dOmdPLnckHrQ8PWgCC7ArS2ppwZ51Y/De93iaihW14kETqCInWwluk9OtkSM6VtYm5kftaeOzMEHOqEiloQNEZ+3C7I0ozY/ZqQpij1OgkuS3jFigtw0FgjtAxG33xPlHt6Ek500bDs1YwAWdDgFJZWAKMLxzFMMRTMXoP++apIoIRK4TUO7U0FzOzkHI3E10LsqDm+yDyPzZug==~3158850~3359814; ak_bmsc=6E35A0309E9B216C0FE5003AFF7C84B5~000000000000000000000000000000~YAAQUGDQF5NsJ3mPAQAATz6Hehfh56Xlpk3I+eaF+XIGqv4CkWMofo0dPxGxFkgRUtgppkgJafcMWyzTFy2ZJ8wg9BTG/UoiWY+rnJyx8iu+jhXpqnfZJHR/JjQ9DV/LRTeuVcIXbGRiD4ceh3pID8MxKhdLkFHqGKhh0H1yFM3Q7a/th8X0dhNLpAzd+34QvsonLebyMiIpdqJ0IBtq6p7d7xlWHoSYuSjY1p85KaqFdGkMhMlkio9J+EYB84+FsjzWWM7/gqcDkHN1Eq6wMZkW00iL8W9P4Bp1t7Fj58Wrx5dFIj+nyJ1sP6BvHVoqjc8nA9Y9/JVRs5Aqw2Wb/58q71SIeAUf4fYhWAxdnh/bEcwEQZ1NT/L4Asbk+OkV4rQOEZoO7AUdaTpuucft8mAXu3mJmFbFfWduZY8N2ct/+gA0/0/tL473bn8lLSx72yQjV9+TPnUzhSSrBXyRmhv1nlDohsaXmNL4nCuN0jCqfnQjsp2gjAaAz8l4DdKNqOjnyUU7M8+0Kl/0; _abck=AC0A6E579954CFC1BA4A76B56A1628F3~0~YAAQUGDQF0M0KHmPAQAAXbiHeguz+Bk9/aUGKxpZCdiBRIGrdmiB4PzaHKBsh3RQAHnu6sajOPfOyg8eEKecBw+r1UqEkKSkjTJn/kQKNuiLLEPOiD/od8VJcxkkUdhpSAS6iKWn/IVxWGpiNYGbeKpnzql3xjOd+/9ehOosRV3BLOinttb5kzIcNhJRmtztBO4y8WBVYXlI7hSFDJf+bC5ehgUNgfwfO6pCq93PXP2qV+hqW+WtCZFLmxmuVh+1/TTE4iESr0soHR0xDhTN4+g5tmKxGQmxzDisshJ31jYmLokM+MIS7VKiAIN4+u9ZOr6KlZfYf4j/5dTSOjcvacwodfIWIBhXdiOLPk4uxu/sTtspU4maDbBj3OG46YmS~-1~-1~-1; bm_sv=112A02ACEC3E4FE9D98440A3DDA9A828~YAAQUGDQF0Q0KHmPAQAAXbiHehd8E/cjYvv1mh5KfWpxoOCFnLoQAi5hpozT/4ujU6FULl3ckrQ7EAkIBmKtlBZap4o48j/rD5L+6tDyClUfsS7YkGq3ofRrH5Xmfk5IusVWG3gqhA0eLxWRbiAz5iW0BiYIAUk3t25tPcP0FzX1I6b7gna4Maqg6imJnj+ultrzUjCY782gbvj+0lM3fczNblNfEInkAw97zmzV4BuOQMHWCe+x1p0C2xLBDKfeRSA=~1; RT="z=1&dm=nseindia.com&si=a746dd1a-fbf2-473e-a100-db75208eb944&ss=lw7bu9bh&sl=1&se=8c&tt=229&bcn=%2F%2F17de4c1b.akstat.io%2F&ld=2p5&nu=kpaxjfo&cl=vw5"; _ga_QJZ4447QD3=GS1.1.1715747629.4.1.1715747672.0.0.0; _ga_87M7PJ3R97=GS1.1.1715747630.5.1.1715747672.0.0.0'
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
st.write("Start Date:",start_date)
st.write("Today's Date:",today)
extract_monthly_futidx_data(start_date,today,selected_option)
