import asyncio
import time
from util.const import *
import numpy as np
import pandas as pd
from strategy.BuySellCheck import *


class DataSyncWorker:
    def __init__(self, kiwoom_class_param, stock_class_param): # param은 stockData 클래스, kiwoom 변수를 가져온다

        print("********************** DataSync 변수 초기 선언 **********************")
        
        self.st_class = stock_class_param
        self.kw_class = kiwoom_class_param

        self.deposit = 0
        self.position = {}

        self.cnt = 0

        # 데이터 초기화 시작!!!
        print('********** 데이터 초기화 시작! **********')

        # 예수금 정보 불러오기
        self.deposit = self.kw_class.get_deposit()

        # 보유종목 가져오기
        self.position = self.kw_class.get_position()
        print(self.position)

        # 주문 내역 가져오기
        self.kw_class.get_order_history()

        # 실시간 조건검색 불러오기
        self.kw_class.get_condition_load()

        # 종목 조건식에 해당하는 종목들 Universe에 세팅함.
        self.check_and_get_universe()

        # 종목코드 : 종목명까지 세팅된 유니버스 딕셔너리 가져와 봄.
        print("DataSync 호출한 유니버스(초기화) :", self.st_class.getUniverse())

        # 실시간 체결정보 가져오기.
        self.set_universe_real_time_price()

        print("********************** 초기화 완료! **********************")


    # 요거 async 핵심
    async def sync_stock_data(self):
        print("********************** Data 최신화 루프 시작! **********************")

        # 실시간 조건검색 불러오기
        self.kw_class.get_condition_load_async()
        time.sleep(1)

        # 종목 조건식에 맞는 종목들 Universe에 세팅함.
        self.check_and_get_universe()

        # 종목코드 : 종목명 세팅된 유니버스 딕셔너리 가져와 봄.
        print("DataSync 호출한 유니버스(루프) :", self.st_class.getUniverse())

        # 종목별 가격정보 가져오기.
        for idx, code in enumerate(self.st_class.getUniverse().keys()):
            print("종목의 가격정보를 가져옵니다. {}/{} : {}".format(idx, len(self.st_class.getUniverse()), code))
            price_df = self.kw_class.get_price_data(code)
            self.st_class.setPrice(code, price_df)

        # 예수금 정보 불러오기
        self.deposit = self.kw_class.get_deposit()

        # 보유종목 가져오기
        self.position = self.kw_class.get_position()
        print(self.position)

        # get_order_history
        self.kw_class.get_order_history()


        print("********************** 매수매도 로직 체크 시작!**********************")
        self.buy_sell_check = BuySellCheck(self.kw_class, self.st_class)
        self.buy_sell_check.buy_sell_check()
        print("********************** 매수매도 로직 체크 종료!**********************")


        print("********************** Data 최신화 루프 종료! **********************")
        print('                                                         ')


    def check_and_get_universe(self):
        real_time_code_list = self.kw_class.code_list
        print("조건검색에 의해 조회된 종목에 종목명을 매칭하겠습니다.")

        for code in real_time_code_list:
            code_name = self.kw_class.get_master_code_name(code)

            # 데이터 객체로 setter 이용해서 변경
            self.st_class.setUniverse(code, code_name)

    def get_price_data_async(self):
        for idx, code in enumerate(self.st_class.getUniverse().keys()):
            print("종목의 가격정보를 가져옵니다. {}/{} : {}".format(idx, len(self.st_class.getUniverse()), code))

            price_df = self.kw_class.get_price_data_async(code)
            #print("가격>>",price_df)
            self.st_class.setPrice(code, price_df)
            

    def set_universe_real_time_price(self):
        fids = get_fid("체결시간")
        codes = self.st_class.getUniverse().keys()
        codes = ";".join(map(str, codes))

        print("실시간 체결현황을 보기 위한 종목코드 묶음 : ", codes)

        self.kw_class.set_real_reg("9999", codes, fids, "0")


    # async로 선언되지 않은 일반 동기 함수 내에서 비동기 함수를 호출하려면 asyncio 라이브러리의 이벤트 루프를 이용해야 함.
    def checkStock(self):
        while True:
            print("                                                   ")
            print("LOOP 횟수>>>>>>>>", self.cnt)
            loop = asyncio.get_event_loop()
            # sync_stock_data 함수 호출해서 계속 갱신
            loop.run_until_complete(self.sync_stock_data())
            loop.close
            self.cnt += 1
            time.sleep(10)