from datetime import date, timedelta
import requests
import zipfile
import io
import pandas as pd
from datetime import date
import datetime
import numpy as np
import streamlit as st
import yfinance as yf

st.set_page_config(
    layout="wide",  # Use the entire screen width
    initial_sidebar_state="collapsed",  # Initially hide the sidebar
)


def extract_monthly_futidx_data(start_date, finish_date, symbol):
    sdate = str(start_date.strftime("%d-%m-%Y"))
    edate = str(finish_date.strftime("%d-%m-%Y"))
    url = "https://www.nseindia.com/api/historical/foCPV?from=" + sdate + "&to=" + edate + "&instrumentType=FUTIDX&symbol=" + symbol
    headers = {
        'authority': 'ww.nseindia.com',
        'method': 'GET',
        'path': "/api/historical/foCPV?from=" + sdate + "&to=" + edate + "&instrumentType=FUTIDX&symbol=" + symbol,
        'scheme': 'ttps',
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Priority": "u=1, i",
        "Referer": "https://www.nseindia.com/report-detail/fo_eq_security",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 "
                      "Safari/537.36",
        'cookie': 'defaultLang=en; _ga=GA1.1.1104812566.1715673932; nseQuoteSymbols=[{"symbol":"NIFTY","identifier":null,"type":"equity"}]; _abck=AC0A6E579954CFC1BA4A76B56A1628F3~0~YAAQNvvaF20olE2PAQAAHzCZewsne/pGIwUxWDzfcJKr6Cs0FONjgZAKaxuhpODz89q2MKtfGaXeydborz355JsyepvooLH4I3zo09HDU6mF9wfsbFx7oOzTWyztTM3RtpbwLhWO4wXstf+eCGOtV0ed+ZQpcimBaxD+WXDAFSQB63xwpoPl0s+9lV7g4ynP3yOGOL2TjKnUtlqoQMWNHWcX8jKpWnJ5zQXbvkjrkk/9x+vf4MR9d6y/yLnUvLDPY2dJYIylpy2hLpY5yGPUTEz0kABeLv2iByQM+EYSlaOo0SZxTVZiIFFRyhsjXJFJS7rt2B1FpC7AlrOlkBj9kAV8oHic+T6cfBDQd3Vgk3ICl74B+3v/GJjOGXo80pSfItG9DdzhdLkkgTpu0nBQn03qryyHxJnbiTg=~-1~-1~-1; nsit=m47LySAoIub5jjA56U_r7ozF; ak_bmsc=6536A6432C9E65D169BD4F72F798E8FF~000000000000000000000000000000~YAAQUGDQF5+LsXmPAQAABGAIfBf+msW3Cw4a3pklDVWsl4xCXns1ZInddhZ0uhy+XnobnbQN72J5W9Pj/ITXkZTh4jZU33dP6qkSbNRBuoz06q4F8tS1tnWge1yJ+75vv+wb26KjNDMwtOEzLpQ9xW8G3BsHp3q4gOzni1KZb7K2rfus+oo8M8VhYZYGA7jEiGwsuesEpcRY2UgSllpi32D3YkCoxzGcFkLS+5bM65bD4TwYceWT4f5hnQXPM1Fbv7rmF4zn9mGszEmuIS7Ak1fDp7yBrSSt7Nkpym+cTKTtStrEn7rNJP9Ql4m3ro9XEZq8KlKmq6ikSPhBzBvCWRjguy/8V8COM9/C7LXBN5suK/OQKV75Jo7Y7SnoIfqcC8kBwBLK5BfjE1B4bJ6zbInhI5eRjHwx; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTcxNTc3Mjg3OCwiZXhwIjoxNzE1NzgwMDc4fQ.Hd3NqMK_Uw_uuvpx4NMLO1HkTBu1R1Fm81AMCsniSbY; AKA_A2=A; bm_sz=1CC02FA83220903BFB53E3FFDDD003FF~YAAQUGDQF0GNsXmPAQAAwm8IfBdSv6stefckYNcErTUYyPin+6liB+PqpfH0zltwtHO7w+5BhvIxbPt4SCpE6cugZsADyXCTD9bWlhdLJGhydZbH7V2EBDYVDPByVQZC7qy87svO9xWMImVg6vdaVEZ+dCxN/OQpVu2jnrg9cc/igys45MaViM9GSzojnM/NBEW4mV/nqCYlKTntSpDJfMpbmV2YfR7QraWbUxmkKI1L9AUSanb+gQ4YnKq0NxEMVVwFrPNHYx7ggv5097zJvwPGKGbu/dH828/6pYbCDDNpmCDLaToN++XbFhCS5U5gx1VZqY2B/cMKDY8hlphY3PeG2BBrY8twva/FrZxbhCLfRqrq9mLdn5QsR+ZBhudx/SpzTEO5yRWcVJ9Og9RnXuP5ogMIf2N/trfXrfDQl1CA9AbEXROr2A==~3225393~3356728; bm_sv=2F5B8C857AD24163945CEF91D4AD04EA~YAAQUGDQF0+PsXmPAQAA/IQIfBd81aolgg3yvC84N9YSveHiG9YZ4uMsDY81efCA7/Gbu5J4ykjKMcV0j2U1OmuJxbHPjRfM4mJdSN9KgI8TIGIwnUbike0x1777swgMGqRHJtspcjBmRj7teEITKyHRtLtCAySjb8cQ2p1w3ciOlTrKPHZi1rfy89p8o+CQ/PQgfxsi1D9vpLV/JjBZCfxt3wK3cyVuxiIYPI4fsvIqKPmV8D3eatphUkF3xUg6Lgk=~1; RT="z=1&dm=nseindia.com&si=a746dd1a-fbf2-473e-a100-db75208eb944&ss=lw7qva9q&sl=2&se=8c&tt=4l3&bcn=%2F%2F17de4c1c.akstat.io%2F&ld=7ne&nu=kpaxjfo&cl=aaw"; _ga_QJZ4447QD3=GS1.1.1715772882.8.0.1715772887.0.0.0; _ga_87M7PJ3R97=GS1.1.1715772872.11.1.1715772887.0.0.0'
        }
    #st.write(url)
    response = pd.DataFrame(requests.get(url, headers=headers).json()['data'])
    #jobj=response.json()
    #reqdata=pd.DataFrame(jobj['data'])
    reqdata = response.drop(['_id', 'TIMESTAMP'], axis=1)
    reqdata = reqdata.rename(columns=lambda x: x[3:])
    reqdata['TIMESTAMP'] = [datetime.datetime.strptime(i, "%d-%b-%Y").date() for i in reqdata['TIMESTAMP']]
    reqdata['EXPIRY_DT'] = [datetime.datetime.strptime(i, "%d-%b-%Y").date() for i in reqdata['EXPIRY_DT']]
    reqdata = reqdata.sort_values(by=['EXPIRY_DT', 'TIMESTAMP'], ignore_index=True)
    reqdata = reqdata.loc[reqdata['EXPIRY_DT'] == list(reqdata['EXPIRY_DT'].unique())[0]]
    futdata = reqdata.drop(
        ['INSTRUMENT', 'OPTION_TYPE', 'MARKET_LOT', 'STRIKE_PRICE', 'MARKET_TYPE', 'TOT_TRADED_QTY', 'TOT_TRADED_VAL'],
        axis=1)
    futdata = futdata[
        ['SYMBOL', 'TIMESTAMP', 'UNDERLYING_VALUE', 'EXPIRY_DT', 'CLOSING_PRICE', 'PREV_CLS', 'LAST_TRADED_PRICE',
         'OPENING_PRICE', 'TRADE_HIGH_PRICE', 'TRADE_LOW_PRICE', 'SETTLE_PRICE', 'OPEN_INT', 'CHANGE_IN_OI']]
    futdata.iloc[:, 4:] = futdata.iloc[:, 4:].astype(float)
    action_setting(futdata,symbol)


def print_curr_val(sym):
    url = "https://www.nseindia.com/api/allIndices"
    headers = {
        'authority': 'www.nseindia.com',
        'method': 'GET',
        'path': "/api/allIndices",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/live-market-indices",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        'cookie': '_ga=GA1.1.1104812566.1715673932; nseQuoteSymbols=[{"symbol":"NIFTY","identifier":null,"type":"equity"}]; _abck=AC0A6E579954CFC1BA4A76B56A1628F3~0~YAAQNvvaF20olE2PAQAAHzCZewsne/pGIwUxWDzfcJKr6Cs0FONjgZAKaxuhpODz89q2MKtfGaXeydborz355JsyepvooLH4I3zo09HDU6mF9wfsbFx7oOzTWyztTM3RtpbwLhWO4wXstf+eCGOtV0ed+ZQpcimBaxD+WXDAFSQB63xwpoPl0s+9lV7g4ynP3yOGOL2TjKnUtlqoQMWNHWcX8jKpWnJ5zQXbvkjrkk/9x+vf4MR9d6y/yLnUvLDPY2dJYIylpy2hLpY5yGPUTEz0kABeLv2iByQM+EYSlaOo0SZxTVZiIFFRyhsjXJFJS7rt2B1FpC7AlrOlkBj9kAV8oHic+T6cfBDQd3Vgk3ICl74B+3v/GJjOGXo80pSfItG9DdzhdLkkgTpu0nBQn03qryyHxJnbiTg=~-1~-1~-1; nsit=m47LySAoIub5jjA56U_r7ozF; ak_bmsc=6536A6432C9E65D169BD4F72F798E8FF~000000000000000000000000000000~YAAQUGDQF5+LsXmPAQAABGAIfBf+msW3Cw4a3pklDVWsl4xCXns1ZInddhZ0uhy+XnobnbQN72J5W9Pj/ITXkZTh4jZU33dP6qkSbNRBuoz06q4F8tS1tnWge1yJ+75vv+wb26KjNDMwtOEzLpQ9xW8G3BsHp3q4gOzni1KZb7K2rfus+oo8M8VhYZYGA7jEiGwsuesEpcRY2UgSllpi32D3YkCoxzGcFkLS+5bM65bD4TwYceWT4f5hnQXPM1Fbv7rmF4zn9mGszEmuIS7Ak1fDp7yBrSSt7Nkpym+cTKTtStrEn7rNJP9Ql4m3ro9XEZq8KlKmq6ikSPhBzBvCWRjguy/8V8COM9/C7LXBN5suK/OQKV75Jo7Y7SnoIfqcC8kBwBLK5BfjE1B4bJ6zbInhI5eRjHwx; AKA_A2=A; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTcxNTc3NjE4MCwiZXhwIjoxNzE1NzgzMzgwfQ.Y3RY9F3otfMHSd-xfpZ_6DT33uHAleQl07FXPb_a4OM; bm_sz=1CC02FA83220903BFB53E3FFDDD003FF~YAAQB2VWaODkaniPAQAA9tE6fBeeckpa8RjqWuYCcaXK1TE3r9nh5uoAtsysEcXBfequnSEa7SLchDFvFGbbfs3NtA3SCbEwTLKeL3DSKL4DvL4qSsWx4lyr6hfKuWL0oNI+MTcJ5ayr+tephB3PrW4BA11osBzf6uddDlzO6f1U/xTJs9Gfb/g7+vPKo5z5CFAvRq7jWWfxEiC3YKg5XZTk+ILC1DDvAsuhZchL/FiR73L37KILiEIbzYJWcPyiS9l1QXKAdJUXhIwsZupy7FQz+m4ffLicykn72stkguPugdFV2gC2iIIuQfNmOBhKTYagUofLzhGmKyOAx2U1URbCWJSg7WY6taQp4EglVceHRJ4cZLX3W3FWxtyJr6TeXuxUS0VfmApWfkbh4LntVDFqc4t4IZcwAIZBsqPFbAFaZxMTdTE6gDY7KTMlRGmoDTDUZFhkcEaiic3IqMoNYH7BNCoWYjJuqsU=~3225393~3356728; defaultLang=en; RT="z=1&dm=nseindia.com&si=a746dd1a-fbf2-473e-a100-db75208eb944&ss=lw7su5tw&sl=1&se=8c&tt=3kk&bcn=%2F%2F17de4c12.akstat.io%2F&ld=5cp&nu=kpaxjfo&cl=dyr"; _ga_QJZ4447QD3=GS1.1.1715772882.8.1.1715776198.0.0.0; _ga_87M7PJ3R97=GS1.1.1715772872.11.1.1715776198.0.0.0; bm_sv=2F5B8C857AD24163945CEF91D4AD04EA~YAAQB2VWaGTlaniPAQAAbR47fBdfP7l4O13XhqIfEszZVZq9FWP5+hNHAq9nXlFRZvmTHGbMMSP414gxFnoqJza0BagDVjKQm0F4n9A4yH2U58HmAvaP11fG0xcqXovF8hWJNjWEoOqlrytN63xRQfXUDx50q3aYQpn+kfkGYbPSfJpbHEdrgAzRSpYc/ZnL7+qWcrFGTJ43ch6oH3K4eckaDl6dheCBVqumaNTVWU0a+dBFNTo0wrNifE6Lcbif2bGWLQ==~1'
        }
    st.write(url)
    response = pd.DataFrame(requests.get(url, headers=headers).json()['data'])
    curr_val = response.loc[response['indexSymbol'] == sym]
    st.write("Underlying Value:", curr_val['last'].item(), "(", np.round(curr_val['variation'].item(), 2), "|",
             curr_val['percentChange'].item(), ")")
    #st.write(np.round(curr_val['variation'].item(),2),curr_val['percentChange'].item())


def action_setting(futdata,symbol):
    exp = futdata['EXPIRY_DT'].unique()
    fdata = pd.DataFrame()
    for i in exp:
        edata = futdata[futdata['EXPIRY_DT'] == i].reset_index(drop=True)
        cat, act, spchange, oichange = [str(np.nan)], [np.nan], [np.nan], [np.nan]
        for j in range(1, len(edata)):
            asp, bsp = edata['SETTLE_PRICE'][j - 1], edata['SETTLE_PRICE'][j]
            spchange.append(np.round(((bsp / asp) - 1) * 100, 2))
            oichange.append(np.round((edata['CHANGE_IN_OI'][j] / edata['OPEN_INT'][j]) * 100, 2))
            sp = [0 if spchange[j] < 0 else 1]
            oi = [0 if oichange[j] < 0 else 1]
            if sp == [1] and oi == [1]:
                cat.append("LB")
                act.append("Buy")
            elif sp == [0] and oi == [1]:
                cat.append("SB")
                act.append("Sell")
            elif sp == [1] and oi == [0]:
                cat.append("SC")
                act.append("Buy")
            elif sp == [0] and oi == [0]:
                cat.append("LL")
                act.append("Sell")
        edata['Settle Price Change'] = spchange
        edata['Movement of OI'] = oichange
        edata['Category'] = cat
        edata['Action'] = act
        fdata = pd.concat([fdata, edata], axis=0)
    fdata = fdata.set_index([pd.Index(range(0, len(fdata)))])
    actfutdata = fdata[['TIMESTAMP', 'UNDERLYING_VALUE', 'EXPIRY_DT', 'SETTLE_PRICE', 'Settle Price Change', 'Movement of OI', 'Category', 'Action']]
    st.dataframe(actfutdata)
    calc_pnl(actfutdata, symbol)


def calc_pnl(actfutdata, symbol):
    today_date = date.today()
    S = today.strftime("%Y-%m-%d")
    test_date = today - timedelta(1)

    shares = pd.read_csv("FNO INDICES.csv")
    share_list = list(shares["SYMBOL"])
    yf_symb = list(shares["YF_SYMB"])[share_list.index(symbol)]
    print("****************YF SYMBOL***************","\n",yf_symb)
    if yf_symb != '':
        data = yf.download(yf_symb, start=today_date, end=today_date + timedelta(1), interval='5m')
        data = pd.DataFrame(data)
        data['Date'] = [i.date() for i in data.index]
        data['Time'] = [i.time() for i in data.index]
        data = data[['Date', 'Time', 'Open', 'High', 'Low', 'Close']].reset_index(drop=True)
        yest_closing = actfutdata.loc[
            (actfutdata['TIMESTAMP'] == test_date) & (actfutdata['EXPIRY_DT'] == actfutdata['EXPIRY_DT'].unique()[0])][
            'UNDERLYING_VALUE'].item()
        action = actfutdata.loc[
            (actfutdata['TIMESTAMP'] == test_date) & (actfutdata['EXPIRY_DT'] == actfutdata['EXPIRY_DT'].unique()[0])][
            'Action'].item()
        if action == 'Buy':
            data['BUY PnL'] = data['Close'] - yest_closing
        elif action == 'Sell':
            data['SELL PnL'] = yest_closing - data['Close']
        st.write(data)
    else:
        st.write("Cash Data Unavailable")



today = date.today()
start_date = today - timedelta(10)
shares = pd.read_csv("FNO INDICES.csv")
share_list = list(shares["SYMBOL"])
selected_option = st.selectbox("Share List", share_list)
st.write("Start Date:", start_date, "Today's Date:", today)
#print_curr_val(list(shares["CURR_VAL_SYMB"])[share_list.index(selected_option)])
extract_monthly_futidx_data(start_date, today, selected_option)

