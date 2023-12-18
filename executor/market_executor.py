# import api libraries
from binance.client import Client as BinClient
import binance.exceptions as bin_ex

# import libraries for logging
import csv
import time
from pathlib import Path
from datetime import datetime, timezone
import pytz

# import libraries for threading
from multiprocessing.pool import ThreadPool

# import config
import executor_config


# executor class
class MarketExecutor(BinClient):
    def __init__(self, exlist, _keys, path_to_data=executor_config.path_to_data, strategy=0):
        super().__init__()
        p = Path(str(executor_config.path_to_data) + '/executor_logging/')
        if not p.exists():
            p.mkdir()

        # setting properties for connection writer
        filename = f'{p}/executor_connection_{strategy}_{datetime.now(timezone.utc).strftime("%Y-%m-%d")}_log.csv'
        _connect_header = ['event_dtm_gmt', 'event_dtm_local', 'exchange_id', 'event_type_id']
        exist = Path(filename).exists()
        self.connect_file = open(filename, 'a', newline='')
        self._connect_writer = csv.writer(self.connect_file)
        # creating connection writer file if not exist
        if not exist:
            self._connect_writer.writerow(_connect_header)
            self.connect_file.flush()

        # setting properties for writer
        filename = f'{p}/executor_{strategy}_{datetime.now(timezone.utc).strftime("%Y-%m-%d")}_log.csv'
        _header = ['gmt_timestamp', 'local_timestamp', 'timestamp_send_orders', 'timestamp_start_execution',
                   'timestamp_execute',
                   'order_id', 'order_type_id', 'exchange_id', 'order_status_id', 'symbol', 'volume',
                   'price_with_commission', 'price']
        exist = Path(filename).exists()
        self.execute_file = open(filename, 'a', newline='')
        self._writer = csv.writer(self.execute_file)
        # creating log writer file if not exist
        if not exist:
            self._writer.writerow(_header)
            self.execute_file.flush()

        # setting properties for error logger
        filename = f'{p}/executor_error_{strategy}_{datetime.now(timezone.utc).strftime("%Y-%m-%d")}_log.csv'
        _error_header = ['gmt_timestamp', 'local_timestamp', 'timestamp_send_orders', 'timestamp_start_execution',
                         'timestamp_execute',
                         'order_id', 'order_type_id', 'exchange_id', 'order_status_id', 'symbol', 'volume',
                         'price_with_commission', 'price', 'error']
        exist = Path(filename).exists()
        self.error_file = open(filename, 'a', newline='')
        self._error_writer = csv.writer(self.error_file)
        # creating error writer file if not exist
        if not exist:
            self._error_writer.writerow(_error_header)
            self.error_file.flush()

        for exchange in exlist:
            if int(exchange) == 0:
                while True:
                    try:
                        # creating binance client
                        self.binance = BinClient(_keys['binance'][0], _keys['binance'][1])
                        self._connect_writer.writerow([*_get_time(), 0, 0])
                        self.connect_file.flush()
                        break
                    except bin_ex.BinanceRequestException:
                        self._connect_writer.writerow([*_get_time(), 0, 1])
                        self.connect_file.flush()
                    finally:
                        self._connect_writer.writerow([*_get_time(), 0, 2])
                        self.connect_file.flush()
    # get balance for specific symbol function
    def get_balance(self, symbol):
        symbol = symbol.upper()
        return self.binance.get_asset_balance(asset=symbol)['free']
    # execute orders vars: timestamp_send_orders,
    # side (0 or 1):
    #   0 - quantity order, to buy or sell particular amount of cryptocurrency
    #   that is first in ticker name (for BTCUSDT it's BTC)
    #   example:
    #       execute(123, 0, [[0, 'BTCUSDT', 1, 10.1]]) will buy 10.1 BTC,
    #       execute(123, 0, [[0, 'BTCUSDT', 0, 10.1]]) will sell 10.1 BTC
    #   1 - quoteOrderQty order, to buy or sell cryptocurrency with particular amount of cryptocurrency
    #   that is second in ticker name (for BTCUSDT it's USDT)
    #   example:
    #       execute(123, 1, [[0, 'BTCUSDT', 1, 10.1]]) will buy BTC at 10.1 USDT,
    #       execute(123, 1, [[0, 'BTCUSDT', 0, 10.1]]) will sell BTC to get 10.1 USDT
    def execute(self, timestamp_send_orders, side, orders_list):
        start_time = round(datetime.now(timezone.utc).timestamp() * executor_config.round_time)  # execution start time
        timestamp_start = []
        timestamp_start.append(start_time)
        timestamp_finish = []
        timestamp_sent = []
        timestamp_sent.append(timestamp_send_orders)
        for order in orders_list:
            exchange_id = int(order[0])
            symbol = order[1]
            side = int(side)
            order_type_id = int(order[2])
            quantity = order[3]
            if exchange_id == 0:
                # if we have more orders than min_order_number orders will execute in Thread
                if len(orders_list) > executor_config.min_order_number:
                    # execution for quantity order
                    if side == 0:
                        pool = ThreadPool(processes=1)
                        async_result = pool.apply_async(self.__binance_market_execute_quantity,
                                                        (symbol, order_type_id, quantity))
                        result = async_result.get()
                        self._writer.writerow([*_get_time(), timestamp_send_orders, start_time, result['timestamp_execute'],
                                               result['order_id'],
                                               order_type_id, exchange_id, result['order_status_id'], symbol, side, quantity,
                                               result['price']])
                        timestamp_finish.append(result['timestamp_execute'])
                        self.execute_file.flush()
                        if result['order_status_id'] == 0:
                            self._error_writer.writerow(
                                [*_get_time(), timestamp_send_orders, start_time, result['timestamp_execute'],
                                 result['order_id'],
                                 order_type_id, exchange_id, result['order_status_id'], symbol, side, quantity, result['price'],
                                 result['error']])
                            timestamp_finish.append(result['timestamp_execute'])
                            self.error_file.flush()
                    # execution for quoteOrderQty order
                    else:
                        pool = ThreadPool(processes=1)
                        async_result = pool.apply_async(self.__binance_market_execute_quoteOrderQty,
                                                        (symbol, order_type_id, quantity))
                        result = async_result.get()
                        self._writer.writerow(
                            [*_get_time(), timestamp_send_orders, start_time, result['timestamp_execute'],
                             result['order_id'],
                             order_type_id, exchange_id, result['order_status_id'], symbol, side, quantity,
                             result['price']])
                        timestamp_finish.append(result['timestamp_execute'])
                        self.execute_file.flush()
                        if result['order_status_id'] == 0:
                            self._error_writer.writerow(
                                [*_get_time(), timestamp_send_orders, start_time, result['timestamp_execute'],
                                 result['order_id'],
                                 order_type_id, exchange_id, result['order_status_id'], symbol, side, quantity,
                                 result['price'],
                                 result['error']])
                            timestamp_finish.append(result['timestamp_execute'])
                            self.error_file.flush()
                # if we have less orders than min_order_number orders will execute in Thread
                else:
                    # execution for quantity order
                    if side == 0:
                        result = self.__binance_market_execute_quantity(symbol, order_type_id, quantity)
                        self._writer.writerow([*_get_time(), timestamp_send_orders, start_time, result['timestamp_execute'],
                                               result['order_id'],
                                               order_type_id, exchange_id, result['order_status_id'], symbol, side, quantity,
                                               result['price']])
                        timestamp_finish.append(result['timestamp_execute'])
                        self.execute_file.flush()
                        if result['order_status_id'] == 0:
                            self._error_writer.writerow(
                                [*_get_time(), timestamp_send_orders, start_time, result['timestamp_execute'],
                                 result['order_id'],
                                 order_type_id, exchange_id, result['order_status_id'], symbol, side, quantity, result['price'],
                                 result['error']])
                            timestamp_finish.append(result['timestamp_execute'])
                            self.error_file.flush()
                    # execution for quoteOrderQty order
                    else:
                        result = self.__binance_market_execute_quoteOrderQty(symbol, order_type_id, quantity)
                        self._writer.writerow(
                            [*_get_time(), timestamp_send_orders, start_time, result['timestamp_execute'],
                             result['order_id'],
                             order_type_id, exchange_id, result['order_status_id'], symbol, side, quantity,
                             result['price']])
                        timestamp_finish.append(result['timestamp_execute'])
                        self.execute_file.flush()
                        if result['order_status_id'] == 0:
                            self._error_writer.writerow(
                                [*_get_time(), timestamp_send_orders, start_time, result['timestamp_execute'],
                                 result['order_id'],
                                 order_type_id, exchange_id, result['order_status_id'], symbol, side, quantity,
                                 result['price'],
                                 result['error']])
                            timestamp_finish.append(result['timestamp_execute'])
                            self.error_file.flush()
        return timestamp_start, timestamp_finish, timestamp_sent
    # executing function for quoteOrderQty orders execute(..., side = 1, ...)
    def __binance_market_execute_quoteOrderQty(self, symbol, order_type_id, quantity):
        order_id = None
        price = None
        timestamp_execute = None
        order_status_id = 1
        exce = None
        if order_type_id == 0:
            try:
                symbol = symbol.upper()

                # creating market sell order
                order = self.binance.create_order(
                    symbol=symbol,
                    side=BinClient.SIDE_SELL,
                    type=BinClient.ORDER_TYPE_MARKET,
                    quoteOrderQty=quantity
                )

                # listening order
                while True:
                    order_info = self.binance.get_order(
                        symbol=symbol,
                        orderId=order['orderId']
                    )
                    if order_info['status'] == 'FILLED':
                        price = order_info['cummulativeQuoteQty']
                        order_status_id = 1
                        break
                    time.sleep(executor_config.sleep_time)  # small sleep time before next loop
                timestamp_execute = round(datetime.now(timezone.utc).timestamp() * executor_config.round_time)  # execution end time
            except Exception as e:
                timestamp_execute = round(datetime.now(timezone.utc).timestamp() * executor_config.round_time)  # execution end time
                order_status_id = 0
        elif order_type_id == 1:
            try:
                symbol = symbol.upper()

                # creating market sell order
                order = self.binance.create_order(
                    symbol=symbol,
                    side=BinClient.SIDE_BUY,
                    type=BinClient.ORDER_TYPE_MARKET,
                    quoteOrderQty=quantity
                )
                # listening order
                while True:
                    order_info = self.binance.get_order(
                        symbol=symbol,
                        orderId=order['orderId']
                    )
                    if order_info['status'] == 'FILLED':
                        price = order_info['cummulativeQuoteQty']
                        order_status_id = 1
                        break
                    time.sleep(executor_config.sleep_time)  # small sleep time before next loop
                order_id = order['orderId']
                timestamp_execute = round(datetime.now(timezone.utc).timestamp() * executor_config.round_time)  # execution end time
            except Exception as e:
                timestamp_execute = round(datetime.now(timezone.utc).timestamp() * executor_config.round_time)  # execution end time
                order_status_id = 0
                exce = e
        result = {'timestamp_execute': timestamp_execute,
                  'order_id': order_id,
                  'order_status_id': order_status_id,
                  'price': price,
                  'error': exce}
        return result

    # executing function for quantity orders execute(..., side = 0, ...)
    def __binance_market_execute_quantity(self, symbol, order_type_id, quantity):
        order_id = None
        price = None
        timestamp_execute = None
        order_status_id = 1
        exce = None
        if order_type_id == 0:
            try:
                symbol = symbol.upper()

                # creating market sell order
                order = self.binance.create_order(
                    symbol=symbol,
                    side=BinClient.SIDE_SELL,
                    type=BinClient.ORDER_TYPE_MARKET,
                    quantity=quantity
                )

                # listening order
                while True:
                    order_info = self.binance.get_order(
                        symbol=symbol,
                        orderId=order['orderId']
                    )
                    if order_info['status'] == 'FILLED':
                        price = order_info['cummulativeQuoteQty']
                        order_status_id = 1
                        break
                    time.sleep(executor_config.sleep_time)  # small sleep time before next loop
                timestamp_execute = round(datetime.now(timezone.utc).timestamp() * executor_config.round_time)  # execution end time
            except Exception as e:
                timestamp_execute = round(datetime.now(timezone.utc).timestamp() * executor_config.round_time)  # execution end time
                order_status_id = 0
        elif order_type_id == 1:
            try:
                symbol = symbol.upper()

                # creating market sell order
                order = self.binance.create_order(
                    symbol=symbol,
                    side=BinClient.SIDE_BUY,
                    type=BinClient.ORDER_TYPE_MARKET,
                    quantity=quantity
                )
                # listening order
                while True:
                    order_info = self.binance.get_order(
                        symbol=symbol,
                        orderId=order['orderId']
                    )
                    if order_info['status'] == 'FILLED':
                        price = order_info['cummulativeQuoteQty']
                        order_status_id = 1
                        break
                    time.sleep(executor_config.sleep_time)  # small sleep time before next loop
                order_id = order['orderId']
                timestamp_execute = round(datetime.now(timezone.utc).timestamp() * executor_config.round_time)  # execution end time
            except Exception as e:
                timestamp_execute = round(datetime.now(timezone.utc).timestamp() * executor_config.round_time)  # execution end time
                order_status_id = 0
                exce = e
        result = {'timestamp_execute': timestamp_execute,
                  'order_id': order_id,
                  'order_status_id': order_status_id,
                  'price': price,
                  'error': exce}
        return result

# auxiliary function for getting time now
def _get_time():
    gmt_time = round(datetime.now(timezone.utc).timestamp() * 1000)
    local_time = round(datetime.now().timestamp() * 1000)
    return [gmt_time, local_time]
