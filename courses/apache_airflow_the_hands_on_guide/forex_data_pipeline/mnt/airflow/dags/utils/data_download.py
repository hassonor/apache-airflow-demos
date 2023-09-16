import csv
import json
import requests


def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/hassonor/6213b86d299beb5ea67ed5d753146f7f/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }

    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            out_data = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                out_data['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(out_data, outfile)
                outfile.write('\n')
