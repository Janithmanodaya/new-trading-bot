import pandas as pd
from binance.client import Client
import os
import csv
from datetime import datetime, timedelta
import requests
import logging

def get_historical_data(client, symbol, interval, start_str, end_str):
    try:
        klines = client.futures_historical_klines(symbol, interval, start_str, end_str)
    except Exception as e:
        logging.warning(f"Could not fetch data for the requested time frame: {e}. Fetching all available data.")
        klines = client.futures_historical_klines(symbol, interval, "1 Jan, 2017")
        
    df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def create_features(df):
    # This is a simplified feature creation process. A real-world application would require more sophisticated feature engineering.
    df['price'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)
    df['is_buyer'] = (df['taker_buy_base_asset_volume'].astype(float) > df['volume'] / 2).astype(int)
    df['delta'] = df['volume'] * (2 * df['is_buyer'] - 1)
    df['cvd'] = df['delta'].cumsum()
    df['imbalance'] = (df['taker_buy_base_asset_volume'].astype(float) / df['volume']).fillna(0.5)
    df['tape_speed'] = df['number_of_trades'] / ((df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]).total_seconds() / 3600)
    df['open_interest'] = 0 # Placeholder - requires open interest data
    df['funding_rate'] = 0 # Placeholder - requires funding rate data
    df['volume_profile'] = 0 # Placeholder - requires volume profile data
    df['success'] = (df['close'].shift(-1) > df['close']).astype(int)
    return df.drop(['open', 'high', 'low', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'], axis=1)

if __name__ == '__main__':
    try:
        client = Client(os.environ.get("BINANCE_API_KEY"), os.environ.get("BINANCE_API_SECRET"))
        client.ping()
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        logging.error(f"Could not connect to Binance API: {e}")
        print("Could not connect to Binance API. Please check your internet connection and API keys.")
        exit()

    with open('symbols.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        symbols = [row[0] for row in reader]

    all_data = []
    for symbol in symbols:
        print(f"Fetching all available data for {symbol}...")
        df = get_historical_data(client, symbol, Client.KLINE_INTERVAL_1HOUR, "1 Jan, 2017", None)
        df = create_features(df)
        all_data.append(df)

    final_df = pd.concat(all_data)
    final_df.to_csv('historical_data.csv', index=False)
    print("Historical data saved to historical_data.csv")
