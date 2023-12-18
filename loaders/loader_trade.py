import os
import time

import websocket

import csv
import json
import config_trade
from datetime import datetime, timezone

my_symbol = 'btcusdt'

stream_name = 'symbol@trade'
stream_name = stream_name.replace('symbol', my_symbol, 1)

columns = ['server_dttm', 'event_dttm', 'symbol_code', 'trade_id', 'price', 'quantity', 'buyer_order_id', 'seller_order_id', 'trade_dttm', 'is_mm_buyer']

sources_dct = config_trade.sources_dct


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

    def on_open_func_default(self):
        return lambda _wsa: _wsa.send(json.dumps(
            {
                "method": "SUBSCRIBE",
                "params":
                    [
                        self.stream
                    ],
                'id': 1
            }
        ))

    def on_message_func_default(self):
        def foo(_wsa, raw_data):

            pre_data = json.loads(raw_data)['data']
            pre_data['s'] = sources_dct[self.ticker]
            pre_data = list(pre_data.values())[1:-1]
            data = [round(datetime.now(timezone.utc).timestamp() * 1000), *pre_data]

            self.cache.append(data)

            if len(self.cache) >= 500 or datetime.now(timezone.utc).hour != self.hour_now:

                if datetime.now(timezone.utc).hour != self.hour_now:
                    self.hour_now = datetime.now(timezone.utc).hour
                    self.day_now = datetime.now(timezone.utc).day
                    self.montn_now = datetime.now(timezone.utc).month
                    self.writer.writerows(self.cache)
                    self.writer = self.writer_func()
                    self.cache.clear()

                self.writer.writerows(self.cache)
                self.cache.clear()
                print(f'writing in {self.stream}, time: {time.asctime()}')
        return foo


def listen_stream(my_obj, url=None, on_open_func=None, on_message_func=None):
    if url is None:
        url = 'wss://stream.binance.com:443/stream'

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

    stream_folder_path = f'{directory}/binance/{stream_name}'
    if not os.path.exists(stream_folder_path):
        os.mkdir(stream_folder_path)
    _filename = f'{stream_folder_path}/{datetime.now(timezone.utc).strftime("%Y_%m_%d_%H")}.csv'

    been_exist = os.path.exists(_filename)
    time.sleep(5)
    _writer = csv.writer(open(_filename, 'a', newline=''))
    if not been_exist:
        _writer.writerow(columns)

    return _writer


def starter():
    if __name__ == '__main__':

        # 4. Prepare
        writer = _writer_maker
        obj = MyParser(stream_name, writer)
        try:
            print('starting up')
            listen_stream(obj)

        except:
            obj.writer.writerows(obj.cache)
            obj.cache.clear()
            print('the button was pressed')
            print('finishing up')

        finally:
            obj.writer.writerows(obj.cache)
            starter()


starter()
