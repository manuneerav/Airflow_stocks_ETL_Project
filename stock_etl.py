import requests
import pandas as pd
import s3fs

def run_stock_etl():

    dict_list = []
    d = {}
    company_list = ["AAPL","TSLA","AMZN","INTC","MSFT","AMD","NVDA","GOOG","AMAT","AVGO","ADP","GILD","TXN","PYPL",
                    "QCOM","INTC","PEP","META","ASML","AZN","NFLX","SNY","PDD","SBUX","INTU","JD","GOOGL","PYPL",
                    "VRTX","ADI"]
    for company in company_list:
        url = f"https://realstonks.p.rapidapi.com/{company}"

        headers = {
        "X-RapidAPI-Key": "55618dbdcamsh65830d26a255f70p1d0efcjsndabaccc30a74",
        "X-RapidAPI-Host": "realstonks.p.rapidapi.com"}


        response = requests.get(url, headers=headers) 
        d = response.json()
        d['name'] = company

        
        dict_list.append(d)

    df = pd.DataFrame.from_dict(dict_list)
    df.to_csv("s3://stock-etl-bucket/stocks.csv",storage_options={'key':'AKIATATOZOAU7SD3VC6W','secret':'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'})
    print(df.head(3))


#print(dict_list)