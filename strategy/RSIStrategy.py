from strategy.DataSyncWorker import DataSyncWorker
from api.StockData import *
from api.Kiwoom import *
import math
import traceback
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from util.notifier import *
from util.const import *


class RSIStrategy:
    def __init__(self):
        self.strategy_name = "RSIStrategy"

        # 키움 객체 인스턴스화
        self.kiwoom = Kiwoom()

        # 데이터 객체 인스턴스화(getter/setter 활용)
        self.stock_data = StockData()

        self.init_strategy()

    def init_strategy(self):
        try:
            print("********************** RSIStrategy 초기화 시작 **********************")

            # LINE 메시지를 보내는 부분
            message = "Auto Trading Bot 프로그램 시작!"
            send_message(message, RSI_STRATEGY_MESSAGE_TOKEN)

            # self.kiwoom.get_condition_load()
            # self.check_get_universe()
            # self.get_price_data()

            # self.kiwoom.get_order_history()
            # self.kiwoom.get_position()

            # self.deposit = self.kiwoom.get_deposit()
            # self.set_universe_real_time_price()

            print("********************** RSIStrategy 초기화 끝 **********************")

        except Exception as e:
            print(traceback.format_exc())

    def thread_run(self):
        # 이걸 전역으로 줘야 가비지 처리를 안함 
        self.worker = DataSyncWorker(self.kiwoom, self.stock_data)
        self.worker.checkStock()








    # def check_get_universe(self):
    #     # 조건식에 해당하는 종목들을 code_list라는 변수에 저장하는 프로세스
    #     # self.kiwoom.get_condition_load()
    #
    #     # problem!!! 비동기식 호출 및 응답이기 때문에 이 사이에 응답해올 때까지 기다리는 무언가가 있어야함. pause 역할.
    #     # code_list를 응답받아오기도 전에 이미 아래 코드들이 실행되니..
    #     # kiwoom.code_list에 실질적으로 담긴 종목들이 없는 상태였던 것.
    #     # 그러니까 그 아래로 쭉 self.universe도.. 그걸 기준으로 가지고 오는 가격정보들도 빈값이었던 것.
    #
    #     # print("code_list 변수에 저장된 코드들입니다 : ", self.kiwoom.code_list)
    #     real_time_code_list = self.kiwoom.code_list
    #     # print("실시간 조회된 종목입니다.", real_time_code_list)
    #
    #     for code in real_time_code_list:
    #         code_name = self.kiwoom.get_master_code_name(code)
    #         # self.universe[code] = {
    #         #     "code_name": code_name
    #         # }
    #         self.stock_data.setUniverse(code, code_name)
    #
    #     print("종목코드가 추가된 유니버스 :", self.universe)

    # def get_price_data(self):
    #     for idx, code in enumerate(self.stock_data.getUniverse().keys()):
    #         print("종목의 가격정보를 가지고 옵니다. {}/{} : {}".format(idx, len(self.stock_data.getUniverse()), code))
    #
    #         price_df = self.kiwoom.get_price_data(code)
    #
    #         # self.universe[code]["price_df"] = price_df
    #         self.stock_data.setPrice(code, price_df)

            # print(self.universe)

    # def set_universe_real_time_price(self):
    #     fids = get_fid("체결시간")

    #     codes = self.stock_data.getUniverse().keys()

    #     codes = ";".join(map(str, codes))

    #     print("실시간 체결현황을 보기 위한 종목코드 묶음 : ", codes)

    #     self.kiwoom.set_real_reg("9999", codes, fids, "0")

    # def check_sell_signal(self, code):
    #     universe_item = self.stock_data.getUniverse()[code]
    #     # print(universe_item)
    #     # print(universe_item.keys())

    #     if code not in self.kiwoom.universe_realtime_transaction_info.keys():
    #         print("매도 대상 확인 과정에서 아직 체결정보가 없습니다.")
    #         return

    #     df = universe_item["price_df"].copy()

    #     # print(df)

    #     # 종가(close) 기준으로 이동평균 구하기. 5일이 아니라 다른 값을 원하면 window 값을 변경
    #     df['ma5'] = df['close'].rolling(window=5, min_periods=1).mean()

    #     # RSI(N) 계산하기.
    #     period = 14
    #     date_index = df.index.astype('str')
    #     U = np.where(df['close'].diff(1) > 0, df['close'].diff(1), 0)
    #     D = np.where(df['close'].diff(1) < 0, df['close'].diff(1) * (-1), 0)
    #     AU = pd.DataFrame(U, index=date_index).rolling(window=period).mean()
    #     AD = pd.DataFrame(D, index=date_index).rolling(window=period).mean()
    #     RSI = AU / (AU + AD) * 100
    #     df['RSI(14)'] = RSI

    #     # 보유종목의 매입가 구하기.
    #     purchase_price = self.kiwoom.position[code]['매입가']

    #     # 보유종목의 현재가 구하기.
    #     close = self.kiwoom.position[code]['현재가']

    #     # df[:-1]은 제일 마지막 행을 의미함. 가장 최근의 ma5, RSI(14) 구하기.
    #     rsi = df[:-1]['ma5'].values[0]
    #     ma5 = df[:-1]['RSI(14)'].values[0]

    #     print("code:{} / signal1:{} / signal2:{} / signal3:{}".format(code, close > ma5, rsi > 80, close > purchase_price * 1.0233))

    #     if close > ma5 and rsi > 80 and close > purchase_price * 1.0233:  # 익절
    #         print("익절 조건에 해당됩니다.")
    #         return True
    #     elif close < purchase_price * 0.98:  # 손절
    #         print("손절 조건에 해당됩니다.")
    #         return True
    #     else:
    #         print("매도 조건을 만족하지 않아 매도를 보류합니다.")
    #         return False

    # def order_sell(self, code):
    #     # 종목명을 가져옴 >> 추후에 메시징과 연동할때 필요해서 갖고옴.
    #     universe_item = self.stock_data.getUniverse()[code]
    #     code_name = universe_item['code_name']

    #     # 보유수량을 확인 함
    #     quantity = self.kiwoom.position[code]['보유수량']
    #     print(quantity)

    #     # 최우선 매도호가를 확인 함
    #     ask = self.kiwoom.universe_realtime_transaction_info[code]['(최우선)매도호가']
    #     print(ask)

    #     # 매도 주문 접수!
    #     order_result = self.kiwoom.send_order('send_sell_order', '1001', 2, code, quantity, ask, '00')
    #     print("매도 주문을 접수합니다! :", order_result)

    # def check_buy_signal_and_order(self, code):
    #     universe_item = self.stock_data.getUniverse()[code]
    #     code_name = universe_item['code_name']

    #     # 현재 체결정보가 존재하지 않는지를 확인.
    #     if code not in self.kiwoom.universe_realtime_transaction_info.keys():
    #         print("매수 대상 확인 과정에서 아직 체결정보가 없습니다.")
    #         return

    #     # 1분봉 가격데이터 밸류를 복사해서 df 변수에 담음
    #     df = universe_item['price_df'].copy()

    #     # 이동평균값 구해서 새로운 열로 추가
    #     df['ma20'] = df['close'].rolling(window=20, min_periods=1).mean()
    #     df['ma60'] = df['close'].rolling(window=60, min_periods=1).mean()

    #     # RSI 구해서 새로운 열로 추가
    #     period = 14
    #     date_index = df.index.astype("str")
    #     U = np.where(df['close'].diff(1) > 0, df['close'].diff(1), 0)
    #     D = np.where(df['close'].diff(1) < 0, df['close'].diff(1) * (-1), 0)
    #     AU = pd.DataFrame(U, index=date_index).rolling(window=period).mean()
    #     AD = pd.DataFrame(D, index=date_index).rolling(window=period).mean()
    #     RSI = AU / (AU + AD) * 100
    #     df['RSI(14)'] = RSI

    #     # df[:-1]은 제일 마지막 행을 의미함. 가장 최근 데이터 기준 잡음.
    #     rsi = df[-1:]['RSI(14)'].values[0]
    #     ma20 = df[-1:]['ma20'].values[0]
    #     ma60 = df[-1:]['ma60'].values[0]

    #     close = self.kiwoom.universe_realtime_transaction_info[code]['현재가']

    #     print("code_name:{} / signal1:{} / signal2:{}".format(code_name, close > ma20 > ma60, rsi <= 50))

    #     # 매수 신호 확인! (매수 조건에 부합한다면 다음 코드로 넘어감)
    #     if close > ma20 > ma60 and rsi <= 50:
    #         budget = round(self.deposit / 10)
    #         bid = self.kiwoom.universe_realtime_transaction_info[code]['(최우선)매수호가']
    #         quantity = round(budget / bid)
    #     # 매수신호에 해당되지 않으면 그대로 함수 종료.
    #     else:
    #         print("매수 신호 조건을 만족하지 않습니다.")
    #         return

    #     # 주문 수량이 1미만이거나 예수금이 0보다 같거나 작으면 그대로 함수 종료.
    #     if quantity < 1 or self.deposit <= 0:
    #         print("주문 수량이 1 미만이거나 예수금이 부족해서 주문 불가합니다.")
    #         return

    #     print("예수금:{}원 / 매수한도금액:{}원 / 매수수량:{}주".format(self.deposit, budget, quantity))

    #     # 매수 주문 접수!
    #     order_result = self.kiwoom.send_order('send_buy_order', '1001', 1, code, quantity, bid, '00')
    #     print("매수 주문을 접수합니다! : ", order_result)


    # """****************************이하 RUN****************************"""

    # for idx, code in enumerate(self.universe.keys()):
    #     print("매수/매도 로직 체크 >> {}/{} : {}/{}".format(idx, len(self.universe), code, self.universe[code]["code_name"]))

        # if code in self.kiwoom.universe_realtime_transaction_info.keys():
        #     print(self.kiwoom.universe_realtime_transaction_info[code])

        # # (1) 접수한 주문이 존재하는지 확인
        # if code in self.kiwoom.order.keys():
        #     # (2) 접수한 주문이 있다면,
        #     print("주문한 내역입니다.", self.kiwoom.order[code])

        #     if code in self.kiwoom.position.keys():
        #         # (2-1) 주문 접수하고 체결돼서 보유한 종목이라면,
        #         print("체결되어 보유한 종목입니다.", self.kiwoom.position[code])

        #         if self.kiwoom.order[code]['미체결수량'] == 0:
        #             print("미체결수량이 0입니다. 매도 가능합니다.")
        #             # (2-2) 매도 로직 실행
        #             if self.check_sell_signal(code):
        #                 self.order_sell(code)
        #         else:
        #             print("미체결수량이 남아있으므로 현재 매도하지 않습니다.")
        #     else:
        #         print("주문했었고 매도를 완료했던 종목이므로 다시 한 번 매수시그널을 체크합니다.")
        #         self.check_buy_signal_and_order(code)

        # # 매수가 체결되어서 접수한 주문이 있는것으로 나오면, 당일에는 계속 매도 조건은 체크 안하네.
        # # 이 사이 과정을 손 볼 필요가 있겠군...

        # # (3) 접수한 주문이 없다면, 보유종목인지 확인
        # elif code in self.kiwoom.position.keys():
        #     print("보유 종목입니다.", self.kiwoom.position[code])
        #     # (6) 매도 조건에 부합하는지 확인
        #     if self.check_sell_signal(code):
        #         # (7) 매도 대상이면 매도 주문 접수!
        #         self.order_sell(code)

        # else:
        #     # (4) 접수 주문도 없고, 보유종목도 아니라면? 매수대상인지 확인 후 주문 접수!
        #     print("접수된 주문도 없고 보유 종목도 아니므로 매수대상인지 체크합니다.")
        #     self.check_buy_signal_and_order(code)

        # time.sleep(2)

    # except Exception as e:
    # print(e)
    # print(traceback.format_exc())


   
