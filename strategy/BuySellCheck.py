import numpy as np
import pandas as pd
import time
import datetime
from util.notifier import *
from util.const import *

class BuySellCheck:
    def __init__(self, kiwoom_class_param, stock_class_param):
        self.st_class = stock_class_param
        self.kw_class = kiwoom_class_param

        self.position = self.kw_class.position
        self.deposit = self.kw_class.deposit
        self.order = self.kw_class.order

    def buy_sell_check(self):
        for idx, code in enumerate(self.st_class.getUniverse().keys()):
            print("매수/매도 로직 체크 >> {}/{} : 종목코드:{} / 종목명:{}".format(idx, len(self.st_class.getUniverse()), code, self.st_class.getUniverse()[code]["code_name"]))
            print(datetime.datetime.now())

            if code in self.kw_class.universe_realtime_transaction_info.keys():
                print(self.kw_class.universe_realtime_transaction_info[code])

            # (1) 접수한 주문이 존재하는지 확인
            if code in self.kw_class.order.keys():
                print("주문한 내역이 존재합니다.", self.kw_class.order[code])
                # (2) 주문한 내역이 있고, 체결이 모두 완료되었는지 확인
                if self.kw_class.order[code]['미체결수량'] == 0:
                    print("미체결수량이 0입니다.")
                    # (2-1) 미체결수량이 0이고 현재 보유한 종목에 있다면 "매수"가 완료된 것
                    if code in self.position.keys():
                        print("매수 완료되어 보유 중인 종목입니다", self.position[code])
                        # (2-2) 전체 수량 보유 중이니 매도 조건에 부합하는지 확인하고 참이면 매도
                        if self.check_sell_signal(code):
                            self.order_sell(code)
                    # (2-3) 미체결수량이 0인데 현재 보유한 종목에 없다면, "매도"를 완료한 것일테니 다시 매수 타점있는지 확인
                    else:
                        self.check_buy_signal_and_order(code)
                # (2-4) 미체결수량이 0이 아니니, 주문 접수하고 부분체결 된 것 또는 아직 아무것도 체결되지 않은 것
                else:
                    print("아직 미체결 수량이 남아 있으므로 체결 되기까지 기다립니다.")

            # (3) 접수한 주문이 없다면, 보유종목인지 확인
            elif code in self.position.keys():
                print("보유 종목입니다.", self.position[code])
                # (3-1) 보유하고 있다면 매도 조건에 부합하는지 확인하고 참이면 매도
                if self.check_sell_signal(code):
                    self.order_sell(code)

            # (4) 접수 주문도 없고 보유종목도 아니라면 매수 대상인지 확인 후 주문 접수!
            else:
                print("접수된 주문도 없고 보유 종목도 아니므로 매수 대상인지 체크합니다.")
                self.check_buy_signal_and_order(code)

            time.sleep(1)


    def check_sell_signal(self, code):
        universe_item = self.st_class.getUniverse()[code]

        if code not in self.kw_class.universe_realtime_transaction_info.keys():
            print("매도 대상 확인 과정에서 아직 체결정보가 없습니다.")
            return

        # df = universe_item["price_df"].copy()

        # 보유종목의 매입가 구하기.
        purchase_price = self.position[code]['매입가']

        # 보유종목의 현재가 구하기.
        close = self.position[code]['현재가']

        # 보유종목의 수익률 구하기.
        profit = self.position[code]['수익률']

        # df[:-1]은 제일 마지막 행을 의미함. 가장 최근의 ma5, RSI(14) 구하기.
        # rsi = df[:-1]['ma5'].values[0]
        # ma5 = df[:-1]['RSI(14)'].values[0]

        # 시그널 확인
        print("code:{} / profit signal:{}".format(code, close > purchase_price * 1.0233))

        # 익절
        if close >= purchase_price * 1.0233:
            print("익절 조건에 해당됩니다.")
            print("매입가:{} / 현재가:{} / 수익률:{}".format(purchase_price, close, profit))
            return True
        # 손절
        elif close < purchase_price * 0.98:
            print("손절 조건에 해당됩니다.")
            print("매입가:{} / 현재가:{} / 손절율:{}".format(purchase_price, close, profit))
            return True
        # 해당 없음
        else:
            print("매도 조건을 만족하지 않아 매도를 보류합니다.")
            return False

    def order_sell(self, code):
        # 종목명을 가져옴 >> 추후에 메시징과 연동할때 필요해서 갖고옴.
        universe_item = self.st_class.getUniverse()[code]
        code_name = universe_item['code_name']

        # 보유수량을 확인 함
        quantity = self.position[code]['보유수량']
        print("현재 보유수량: ", quantity)

        # 최우선 매도호가를 확인 함
        ask = self.kw_class.universe_realtime_transaction_info[code]['(최우선)매도호가']
        print("최우선매도호가: ", ask)

        # 매도 주문 접수!
        order_result = self.kw_class.send_order('send_sell_order', '1001', 2, code, quantity, ask, '00')
        print("매도 주문을 접수합니다! :", order_result)

        # LINE 메시지를 보내는 부분
        message = "[{}] 매도 주문 접수 완료! 수량:{}, 매도호가:{}".format(code_name, quantity, ask)
        send_message(message, RSI_STRATEGY_MESSAGE_TOKEN)

    def check_buy_signal_and_order(self, code):
        universe_item = self.st_class.getUniverse()[code]
        code_name = universe_item['code_name']

        # 현재 체결정보가 존재하지 않는지를 확인.
        if code not in self.kw_class.universe_realtime_transaction_info.keys():
            print("매수 대상 확인 과정에서 아직 체결정보가 없습니다.")
            return

        # 한번 거래한 종목은 다시 거래 안함.
        if code in self.order:
            print("오늘 이미 매수 주문을 했던 내역이므로 리턴!")
            return

        # 1분봉 가격데이터 value를 복사해서 df 변수에 담음
        df = universe_item['price_df'].copy()

        # 이동평균값 구해서 새로운 열로 추가
        df['ma120'] = df['close'].rolling(window=120, min_periods=1).mean()
        ma120 = df[-1:]['ma120'].values[0]

        # 실시간 현재가 가져오기
        close = self.kw_class.universe_realtime_transaction_info[code]['현재가']

        # 2차 거름망
        if (df['VM'].max() > 140 and df['high'].max() >= df['open'][0] * 1.09) or (df['VM'].max() > 90 and df['high'].max() >= df['open'][0] * 1.15):
            print("VM max :", df['VM'].max(), "/ 당일 고가 :", df['high'].max(), "/ 시가9%상승 :", df['open'][0] * 1.09, "/ 시가15%상승 :", df['open'][0] * 1.15)
            print("df['VM'].max() > 140 and df['high'].max() >= df['open'][0] * 1.09 조건 :", df['VM'].max() > 140 and df['high'].max() >= df['open'][0] * 1.09)
            print("df['VM'].max() > 90 and df['high'].max() >= df['open'][0] * 1.15 조건 :", df['VM'].max() > 90 and df['high'].max() >= df['open'][0] * 1.15)
            print("2차 거름망 통과!!")
        else:
            print("VM max :", df['VM'].max(), "/ 당일 고가 :", df['high'].max(), "/ 시가9%상승 :", df['open'][0] * 1.09, "/ 시가15%상승 :", df['open'][0] * 1.15)
            print("df['VM'].max() > 140 and df['high'].max() >= df['open'][0] * 1.09 조건 :", df['VM'].max() > 140 and df['high'].max() >= df['open'][0] * 1.09)
            print("df['VM'].max() > 90 and df['high'].max() >= df['open'][0] * 1.15 조건 :", df['VM'].max() > 90 and df['high'].max() >= df['open'][0] * 1.15)
            print("2차 거름망 조건(VM / 상승률) 미충족으로 리턴!")
            return

        # 추가 거름망
        if close >= ma120:
            print("이동평균선(120) 조건 통과!!")
        else:
            print("이동평균선(120) 조건 미충족으로 리턴!!")
            return

        # 양수인 빈덱스값만 따로 dataframe 생성
        df1 = df.loc[(df['Vindex'] > 0), ['date', 'Vindex']]
        print("거름망 조건 모두 통과하여 Vindex DataFrame 따로 생성")

        # 1타점 거래조건 (이전 계단에서 매수)
        print("*** 1타점 거래 조건 충족하는지 확인 ***")
        try:
            for i in range(1, len(df1.index) + 1):
                x = i + 1
                # 400은 4분 이상이 지속되었다는 것을 의미함.
                if int(df1.iloc[-i]['date']) - int(df1.iloc[-x]['date']) >= 400:
                    if int(df1.iloc[-1]['Vindex']) >= int(df1.iloc[-x]['Vindex']) * 1.03:
                        if int(df1.iloc[-x]['Vindex']) * 0.995 <= close <= int(df1.iloc[-x]['Vindex']) * 1.005:
                            print("1타점 매수조건 충족!")
                            budget = round(int(self.deposit) / 10)
                            bid = self.kw_class.universe_realtime_transaction_info[code]['(최우선)매수호가']
                            quantity = round(budget / bid)

                            if quantity < 1 or int(self.deposit) <= 0:
                                print("주문 수량이 1 미만이거나 예수금이 부족해서 주문 불가합니다.")
                                return

                            print("예수금:{}원 / 매수한도금액:{}원 / 매수수량:{}주".format(int(self.deposit), budget, quantity))

                            # 매수 주문 접수!
                            # order_result = self.kw_class.send_order('send_buy_order', '1001', 1, code, quantity, bid, '00')
                            order_result = self.kw_class.send_order('send_buy_order', '1001', 1, code, quantity, 0, '03')
                            print("[1타점] 매수 주문을 접수합니다! : ", order_result)

                            # LINE 메시지를 보내는 부분
                            message = "[{}] [1타점] 매수 주문 접수 완료! 수량:{}, 매수가격:{} 부근".format(code_name, quantity, bid)
                            send_message(message, RSI_STRATEGY_MESSAGE_TOKEN)

                            break

                        print("1타점 매수조건 중 현재가가 아직 계단수준까지 도달하지 않았음.")
                        break
                    print("1타점 매수조건 중 3% 상승 기준을 충족하지 못하였음.")
                    break
                else:
                    continue
        except IndexError:
            print("계단 4분 조건을 끝까지 만족하지 못하였으므로 패스")
            pass

        # 2타점 거래 (현재 계단 다시 터치하면 매수)
        print("*** 2타점 거래 조건 충족하는지 확인 ***")
        if df['close'].max() >= int(df1.iloc[-1]['Vindex']) * 1.06:
            if int(df1.iloc[-1]['Vindex']) * 0.095 <= close <= int(df1.iloc[-1]['Vindex']) * 1.005:
                budget = round(int(self.deposit) / 100)
                bid = self.kw_class.universe_realtime_transaction_info[code]['(최우선)매수호가']
                quantity = round(budget / bid)
                print("2타점 매수조건 충족!")

                if quantity < 1 or int(self.deposit) <= 0:
                    print("주문 수량이 1 미만이거나 예수금이 부족해서 주문 불가합니다.")
                    return

                print("예수금:{}원 / 매수한도금액:{}원 / 매수수량:{}주".format(int(self.deposit), budget, quantity))

                # 매수 주문 접수!
                # order_result = self.kw_class.send_order('send_buy_order', '1001', 1, code, quantity, bid, '00')
                order_result = self.kw_class.send_order('send_buy_order', '1001', 1, code, quantity, 0, '03')
                print("[2타점] 매수 주문을 접수합니다! : ", order_result)

                # LINE 메시지를 보내는 부분
                message = "[{}] [2타점] 매수 주문 접수 완료! 수량:{}, 매수가격:{} 부근".format(code_name, quantity, bid)
                send_message(message, RSI_STRATEGY_MESSAGE_TOKEN)

            else:
                print("2타점 매수조건 중 현재가가 아직 현재 계단수준까지 도달하지 않았음.")
        else:
            print("2타점 매수조건 중 현재 계단보다 주가가 6% 이상 오르지 않았음.")

        # 3타점 거래 (VI 기준)
        print("*** 3타점 거래 조건 충족하는지 확인 ***")
        if df['VM'].max() > 140 and df['open'][0] * 1.12 <= df['close'].max() <= df['open'][0] * 1.14:
            if df['open'][0] * 1.095 <= close <= df['open'][0] * 1.105:
                budget = round(int(self.deposit) / 100)
                bid = self.kw_class.universe_realtime_transaction_info[code]['(최우선)매수호가']
                quantity = round(budget / bid)
                print("3타점 매수조건 충족!")

                if quantity < 1 or int(self.deposit) <= 0:
                    print("주문 수량이 1 미만이거나 예수금이 부족해서 주문 불가합니다.")
                    return

                print("예수금:{}원 / 매수한도금액:{}원 / 매수수량:{}주".format(int(self.deposit), budget, quantity))

                # 매수 주문 접수!
                # order_result = self.kw_class.send_order('send_buy_order', '1001', 1, code, quantity, bid, '00')
                order_result = self.kw_class.send_order('send_buy_order', '1001', 1, code, quantity, 0, '03')
                print("[3타점] 매수 주문을 접수합니다! : ", order_result)

                # LINE 메시지를 보내는 부분
                message = "[{}] [3타점] 매수 주문 접수 완료! 수량:{}, 매수가격:{} 부근".format(code_name, quantity, bid)
                send_message(message, RSI_STRATEGY_MESSAGE_TOKEN)

            else:
                print("3타점 매수조건 중 현재가가 아직 1차 VI 가격 수준까지 도달하지 않았음.")
        else:
            print("3타점 매수조건 중 VM과 VI 기준을 통과하지 못하였음.")

        # 아무것도 해당안된다면 그대로 리턴!
        print("*** 1,2,3타점 거래 조건 모두 체크 완료 ***")
        return

        # # 아무것도 해당안된다면 그대로 리턴!
        # else:
        #     print("매수 신호 조건을 어느것도 만족하지 않습니다.")
        #     return
        #
        # # 주문 수량이 1 미만이거나 예수금이 0보다 같거나 작으면 그대로 함수 종료.
        # if quantity < 1 or int(self.deposit) <= 0:
        #     print("주문 수량이 1 미만이거나 예수금이 부족해서 주문 불가합니다.")
        #     return
        #
        # print("예수금:{}원 / 매수한도금액:{}원 / 매수수량:{}주".format(int(self.deposit), budget, quantity))
        #
        # # 매수 주문 접수!
        # order_result = self.kw_class.send_order('send_buy_order', '1001', 1, code, quantity, bid, '00')
        # print("매수 주문을 접수합니다! : ", order_result)