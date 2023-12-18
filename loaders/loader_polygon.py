import config_trade
import os
import time

import websocket

import csv
import json
from datetime import datetime, timezone
# selected symbol (entered automatically by runner.py)
my_symbol = 'SPX'

stream_name = my_symbol


columns = ['server_dttm', 'price', 'symbol_code', 'event_dttm']
str_columns = ''
for i in range(len(columns)):
    if i == len(columns) - 1:
        str_columns += columns[i]
    else:
        str_columns += columns[i] + ','
sources_dct = {'btcusdt': 1, 'btctusd': 2, 'ethusdt': 3, 'ethbtc': 4, 'ethbusd': 5, 'eurusdt': 6, 'ethrub': 7,
               'btcrub': 8, 'etheur': 9, 'btceur': 10, 'ethgbp': 11, 'btcgbp': 12, 'btcbusd': 13, 'ethtusd': 14,
               'SPX': 15, 'NDX': 16}


class MyParser:

    def __init__(self, stream, writer_func):
        self.stream = stream
        self.cache = []
        self.writer_func = writer_func
        self.writer = self.writer_func()
        self.ticker = stream.split('@')[0]
        self.hour_now = datetime.now(timezone.utc).hour
        self.day_now = datetime.now(timezone.utc).day
        self.montn_now = datetime.now(timezone.utc).month
        self.n = 1
        self.flag = True

    def on_open_func_default(self):
        def foo(_wsa):
            # authentication
            header = json.dumps(
                {
                    "action": "auth",
                    "params": "ZgCwpZsU6l96RJii4s1V1YwxWovzk0mG"
                }
            )
            # send subscribe request
            sub = json.dumps(
                {
                    "action": "subscribe",
                    "params": f"V.I:{self.stream}"
                }
            )
            # stop sending authentication if it sent
            if self.flag:
                _wsa.send(header)
                self.flag = False
            _wsa.send(sub)
        # this function needs to return another function, cause WebSocketApp needs at least two functions
        return foo

    def on_message_func_default(self):
        def foo(_wsa, raw_data):
            pre_data = json.loads(raw_data)[0]
            if datetime.now(timezone.utc).hour > self.hour_now:
                self.writer = self.writer_func()
                self.hour_now = datetime.now(timezone.utc).hour
            print(pre_data)
            if "status" not in pre_data.keys():
                # replace the symbol with the code
                pre_data['T'] = sources_dct[self.ticker]
                # we don't take first (there is useless code)
                pre_data = list(pre_data.values())[1:]
                # union of server time and received data
                data = [round(datetime.now(timezone.utc).timestamp() * 1000), *pre_data]

                self.cache.append(data)
                # here we need to check if day is changed
                if len(self.cache) >= 500 or datetime.fromtimestamp(pre_data[-1] / 1000).hour != self.hour_now:

                    if datetime.fromtimestamp(pre_data[-1] / 1000).hour != self.hour_now:
                        self.hour_now = datetime.fromtimestamp(pre_data[-1] / 1000).hour
                        self.day_now = datetime.fromtimestamp(pre_data[-1] / 1000).day
                        self.montn_now = datetime.fromtimestamp(pre_data[-1] / 1000).month
                        self.writer.writerows(self.cache)
                        self.writer = self.writer_func()
                        self.cache.clear()

                    self.writer.writerows(self.cache)
                    self.cache.clear()
                    print(f'writing in {self.stream}, time: {time.asctime()}')
        return foo


def listen_stream(my_obj, url=None, on_open_func=None, on_message_func=None):
    if url is None:
        url = 'wss://delayed.polygon.io/indices'

    if on_open_func is None:
        on_open_func = my_obj.on_open_func_default()

    if on_message_func is None:
        on_message_func = my_obj.on_message_func_default()
    wsapp = websocket.WebSocketApp(url=url, on_open=on_open_func, on_message=on_message_func)
    wsapp.run_forever(ping_interval=40, reconnect=0)


def _writer_maker():
    time.sleep(5)
    directory = config_trade.directory
    # stream_name = config_trade.stream_name
    print(f'Selected directory: {directory}')
    print(f'Selected stream: {stream_name}')
    asset_folder_path = f'{directory}/polygon'
    if not os.path.exists(asset_folder_path):
        os.mkdir(asset_folder_path)
    stream_folder_path = f'{directory}/polygon/{stream_name}'
    if not os.path.exists(stream_folder_path):
        os.mkdir(stream_folder_path)
    _filename = f'{stream_folder_path}/{datetime.now(timezone.utc).strftime("%Y_%m_%d_%H")}.csv'
    been_exist = os.path.exists(_filename)
    my_writer = csv.writer(open(_filename, 'a', newline=''))
    time.sleep(5)
    if not been_exist:
        with open(_filename, 'a') as file:
            file.write(str_columns + '\r\n')
        #my_writer.writerow(columns)
        print("r")
    return my_writer


def starter():
    if __name__ == '__main__':

        # 4. Prepare
        writer = _writer_maker
        obj = MyParser(stream_name, writer)
        try:
            print('starting up')
            listen_stream(obj)

        except Exception as e:
            obj.writer.writerows(obj.cache)
            obj.cache.clear()
            print(e)
            print('the button was pressed')
            print('finishing up')

        finally:
            obj.writer.writerows(obj.cache)
            starter()


starter()
