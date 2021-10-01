from PyQt5.QAxContainer import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
import pandas as pd
import numpy as np
import time
from util.const import *
import datetime
import asyncio



class Kiwoom(QAxWidget):
    def __init__(self):
        super().__init__()
        self._make_kiwoom_instance()
        self._set_signal_slots()
        self._comm_connect()

        # 계좌번호 저장(모의투자는 8로시작, 실계좌는 5로시작)
        self.account_number = self.get_account_number()

        # TR에 사용할 event_loop 변수
        self.tr_event_loop = QEventLoop()

        # 당일 전체 주문 목록
        self.order = {}

        # 유니버스 종목의 실시간 체결 데이터 정보
        self.universe_realtime_transaction_info = {}

        # 조건검색식에 해당되는 종목코드 리스트
        self.code_list = []


    # 키움 API 사용을 등록하는 함수
    def _make_kiwoom_instance(self):
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

    # API로 보내는 요청들에 대한 응답을 받아올 슬롯 공간을 만드는 함수
    def _set_signal_slots(self):
        # 로그인 응답의 결과를 _login_slot을 통해 받도록 설정
        self.OnEventConnect.connect(self._login_slot)

        # TR의 응답결과를 _on_receive_tr_data을 통해 받도록 설정
        self.OnReceiveTrData.connect(self._on_receive_tr_data)
        
        # 주문넣고 체결 결과를 _on_chejan_slot을 통해 받도록 설정 (send_order)
        self.OnReceiveChejanData.connect(self._on_chejan_slot)

        # 실시간 체결정보를 _on_receive_real_data를 통해 받도록 설정 (set_real_reg)
        self.OnReceiveRealData.connect(self._on_receive_real_data)

        # 조건검색식을 요청하면 _on_receive_condition_ver를 통해 응답 (get_condition_load)
        self.OnReceiveConditionVer.connect(self._on_receive_condition_ver)

        # 요청한 조건검색식에 해당되는 실시간 종목코드 정보를 응답
        self.OnReceiveTrCondition.connect(self._on_receive_tr_condition)

    # 조건검색식을 응답받는 slot 함수
    def _on_receive_condition_ver(self):
        condition_list = {'index': [], 'name': []} # ex) 001^인기천조건;002^편도현조건;
        temporary_condition_list = self.dynamicCall("GetConditionNameList()").split(";")[:-1]
        time.sleep(2)
        print("컨디션 리스트 : ", temporary_condition_list)

        for data in temporary_condition_list:
            a = data.split("^")
            condition_list['index'].append(str(a[0]))
            condition_list['name'].append(str(a[1]))

        n_index = int(condition_list['index'][0])
        condition_name = str(condition_list['name'][0])

        iret = self.dynamicCall("SendCondition(QString, QString, int, int)", "0156", condition_name, n_index, 0)
        time.sleep(2)

        if iret == 1:
            print("조건검색식 조회 요청 성공")
        elif iret != 1:
            print("조건검색식 조회 요청 실패")

    # 조건검색식에 해당되는 종목코드 정보를 응답받아 변수에 저장하는 함수
    def _on_receive_tr_condition(self, scrno, codelist, condition_name, n_index, nnext):
        temporary_code_list = codelist.split(";")[:-1]
        print("_on_receive_tr_condition!!")
        for code in temporary_code_list:
            # 중복제거
            if code not in self.code_list:
                self.code_list.append(code)

        print("조회된 종목 리스트: ", self.code_list)

        self.condition_loop.exit()

    # 로그인 요청 시그널을 보내는 함수
    def _comm_connect(self):
        self.dynamicCall("commConnect()")
        #로그인 응답대기 시작
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()

    # 로그인 응답의 결과를 얻는 함수
    def _login_slot(self, err_code):
        if err_code == 0:
            print("접속 성공!")
        else:
            print("연결 실패...")
        self.login_event_loop.exit()

    # 주문접수/체결 정보를 얻는 함수
    def _on_chejan_slot(self, s_gubun, n_item_cnt, s_fid_list):
        """
        실시간 체결정보 수신 slot 함수
        s_gubun >> 0:주문체결, 1:잔고, 3:특이신호
        n_item_count >> 주문접수, 체결이 이루어질 때 얻을 수 있는 정보의 항목 수. 각자 고유한 FID값을 가지고 있음. ex>주문번호 FID는 913
        s_fid_list >> 해당 응답에서 얻을 수 있는 데이터(fid)
        """

        print('_on_chejan_slot')
        print(s_gubun, n_item_cnt, s_fid_list)

        if int(s_gubun) == 0:
            # 9001-종목코드 얻어오기, 종목코드는 A007700처럼 오기 때문에 앞자리를 제거함
            code = self.dynamicCall("GetChejanData(int)", '9001')[1:]

            # order dictionary에 코드정보가 없다면 신규생성
            if code not in self.order.keys():
                self.order[code] = {}

            # 9201;9203;9205;9001;912;913;302;900;901; 이런 식으로 전달되는 fid이 리스트를 ';' 기준으로 구분함
            for fid in s_fid_list.split(";"):
                if fid in FID_CODES:
                    data = self.dynamicCall("GetChejanData(int)", fid)

                    # 데이터에 +,-가 붙어있는 경우 (ex:+매수, -매도) 제거
                    data = data.strip().lstrip('+').lstrip('-')

                    # fid 코드에 해당하는 key값을 찾음(ex: fid=9201 > key:계좌번호)
                    key = FID_CODES[fid]

                    # 수신한 데이터는 전부 문자형인데 문자형 중에 숫자인 항목들(ex:매수가)은 숫자로 변형이 필요함
                    if data.isdigit():
                        data = int(data)

                    # print("주문 Chejan 관련 로그 >> {}: {}".format(key, data))

                    # code를 key값으로해서 항목들을 저장
                    self.order[code][key] = data

        # 저장한 order 출력
        print(self.order)

    # 실시간 체결정보 응답받는 실질적인 slot 함수
    def _on_receive_real_data(self, s_code, real_type, real_data):
        # print("on_receive_real_data!", s_code, real_type)

        if real_type == "장시작시간":
            pass
        elif real_type == "주식체결":
            signed_at = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("체결시간")) # HHMMSS
            close = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("현재가")) # +(-)7000
            high = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("고가")) # +(-)7000
            open = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("시가")) # +(-)7000
            low = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("저가")) # +(-)7000
            top_priority_ask = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("(최우선)매도호가")) # +(-)7000
            top_priority_bid = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("(최우선)매수호가")) # +(-)7000
            accum_volume = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("누적거래량")) # 출력 : 77777

            # 불러온 데이터들의 형 변환 작업
            close = abs(int(close))
            high = abs(int(high))
            open = abs(int(open))
            low = abs(int(low))
            top_priority_ask = abs(int(top_priority_ask))
            top_priority_bid = abs(int(top_priority_bid))
            accum_volume = abs(int(accum_volume))

            # # 연월일을 가져오는 작업
            # date_today = datetime.date.today()
            # print(type(date_today))
            #
            # # 연월일 사이에 있는 '-'를 빼주는 작업
            # date_today = str(date_today).replace("-", "")
            # print(date_today)
            #
            # # 년월일 + 시분초 붙이는 작업
            # new_signed_at = date_today + signed_at

            # print(new_signed_at, close, high, open, low, top_priority_ask, top_priority_bid, accum_volume)

            #code를 key값으로 한 dictionary 변환
            self.universe_realtime_transaction_info[s_code] = {
                "체결시간": signed_at,
                "시가": open,
                "고가": high,
                "저가": low,
                "현재가": close,
                "(최우선)매도호가": top_priority_ask,
                "(최우선)매수호가": top_priority_bid,
                "누적거래량": accum_volume
            }

            # df1 = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
            #
            # df1.loc[new_signed_at] = [open, high, low, close, accum_volume]
            # print(df1)
            #
            # result = self.df_save.append(df1)
            # print(result)



    # 응답받는 slot 함수들 모음
    def _on_receive_tr_data(self, screen_no, rqname, trcode, record_name, next, unused1, unused2, unused3, unused4):

        print("[Kiwoom] _on_receive_tr_data is called 화면번호:{} / rq별명:{} / tr코드:{}".format(screen_no, rqname, trcode))
        tr_data_cnt = self.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
        print("tr_data_cnt>>>>", tr_data_cnt)

        # 현재시간 - 장시작시간 분 단위 카운트
        # now = datetime.datetime.now()
        # base_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        # time_gap = now - base_time
        # time_gap_min = time_gap.seconds / 60
        # time_gap_min = int(time_gap_min)
        # print(time_gap_min)

        # if next == '2':
        #     self.has_next_tr_data = True
        # else:
        #     self.has_next_tr_data = False

        # 분봉 가격 데이터 불러오기
        if rqname == "opt10080_req":
            ohlcv = {'date': [], 'open': [], 'high': [], 'low': [], 'close': [], 'volume': []}

            # 900개 전체 데이터 불러오기
            time.sleep(1)
            for i in range(tr_data_cnt):
                date = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "체결시간")
                open = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "시가")
                high = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "고가")
                low = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "저가")
                close = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "현재가")
                volume = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "거래량")

                ohlcv['date'].append(date.strip())
                ohlcv['open'].append(abs(int(open)))
                ohlcv['high'].append(abs(int(high)))
                ohlcv['low'].append(abs(int(low)))
                ohlcv['close'].append(abs(int(close)))
                ohlcv['volume'].append(abs(int(volume)))

            # 각 value 값들을 역으로 바꿈 >> 제일 먼 과거 데이터가 가장 앞 순서로 오게 됨.
            ohlcv['date'].reverse()
            ohlcv['open'].reverse()
            ohlcv['high'].reverse()
            ohlcv['low'].reverse()
            ohlcv['close'].reverse()
            ohlcv['volume'].reverse()

            # print(ohlcv)

            # 오늘 09시 00분 00초 시작 지점. today_start_index 구하는 로직
            now = datetime.datetime.now()
            base_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
            str_base_time = base_time.strftime("%Y%m%d%H%M%S")

            try:
                today_start_index = ohlcv['date'].index(str_base_time)
                print(today_start_index)
            except:
                base_time = now.replace(hour=9, minute=2, second=0, microsecond=0)
                str_base_time = base_time.strftime("%Y%m%d%H%M%S")
                today_start_index = ohlcv['date'].index(str_base_time)
                print(today_start_index)

            # 오늘 값들을 담을 새로운 ohlcv 딕셔너리 생성
            new_ohlcv = {'date': [], 'open': [], 'high': [], 'low': [], 'close': [], 'volume': [], 'close_diff': [],
                     'A_value': [], 'B_value': [], 'F_value': [], 'G_value': [], 'D_value': [], 'I_value': [], 'VM_1min': [], 'VM': []}

            # close_b4 초기값 선언
            close_b4 = 0

            # 오늘 09시 00분 00초 index 부터 최종 인덱스까지 for문 돌면서 A, B, F, G, D, I, VM_1min 값 계산하여 담기
            for i in range(today_start_index, tr_data_cnt):
                new_date = ohlcv['date'][i]
                new_open = ohlcv['open'][i]
                new_high = ohlcv['high'][i]
                new_low = ohlcv['low'][i]
                new_close = ohlcv['close'][i]
                new_volume = ohlcv['volume'][i]

                open_v = abs(int(new_open))
                high_v = abs(int(new_high))
                low_v = abs(int(new_low))
                close_v = abs(int(new_close))
                volume_v = abs(int(new_volume))

                if i == today_start_index:
                    A_value_max = 1
                    close_diff = 0
                else:
                    close_diff = close_v - close_b4

                close_b4 = close_v
                A_value = close_diff * volume_v

                if A_value > A_value_max:
                    A_value_max = A_value

                if i < today_start_index + 3:
                    F_value = 1
                    F_value_max = F_value
                else:
                    F_value = close_diff * volume_v
                    if F_value > F_value_max:
                        F_value_max = F_value

                D_value = A_value / A_value_max * 100

                # VM 값을 구하기 위한 기초 작업
                U_D_flag = new_close - new_open
                VM1 = 0
                VM2 = 0

                if U_D_flag > 0:
                    VM1 = (new_open + new_high + new_close + new_low) / 4 * new_volume
                elif U_D_flag < 0:
                    VM2 = -(new_open + new_high + new_close + new_low) / 4 * new_volume

                VM_1min = (VM1 + VM2) / 100000000

                if A_value_max <= F_value_max:
                    I_value = 50
                else:
                    I_value = 19

                new_ohlcv['date'].append(new_date.strip())
                new_ohlcv['open'].append(abs(int(new_open)))
                new_ohlcv['high'].append(abs(int(new_high)))
                new_ohlcv['low'].append(abs(int(new_low)))
                new_ohlcv['close'].append(abs(int(new_close)))
                new_ohlcv['volume'].append(abs(int(new_volume)))
                new_ohlcv['close_diff'].append(int(close_diff))
                new_ohlcv['A_value'].append(int(A_value))
                new_ohlcv['B_value'].append(int(A_value_max))
                new_ohlcv['F_value'].append(int(F_value))
                new_ohlcv['G_value'].append(int(F_value_max))
                new_ohlcv['D_value'].append(int(D_value))
                new_ohlcv['I_value'].append(int(I_value))
                new_ohlcv['VM_1min'].append(float(VM_1min))

            VM = 0
            for i in range(len(new_ohlcv['VM_1min'])):
                VM = VM + new_ohlcv['VM_1min'][i]
                new_ohlcv['VM'].append(float(VM))

            self.tr_data = new_ohlcv

            self.tr_event_loop.exit()
            time.sleep(0.5)

        elif rqname == "opt10080_req_async":
            ohlcv = {'date': [], 'open': [], 'high': [], 'low': [], 'close': [], 'volume': []}

            for i in range(tr_data_cnt):
                date = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "체결시간")
                open = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "시가")
                high = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "고가")
                low = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "저가")
                close = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "현재가")
                volume = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "거래량")

                ohlcv['date'].append(date.strip())
                ohlcv['open'].append(abs(int(open)))
                ohlcv['high'].append(abs(int(high)))
                ohlcv['low'].append(abs(int(low)))
                ohlcv['close'].append(abs(int(close)))
                ohlcv['volume'].append(abs(int(volume)))

            # 추가함
            self.tr_data = ohlcv
            time.sleep(1)
                
        # 예수금 불러오기
        elif rqname == "opw00001_req":
            self.deposit = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, 0, "예수금")
            time.sleep(1)

            self.tr_data = int(self.deposit)
            print("현재 예수금은 : ", self.tr_data, "원")

            self.tr_event_loop.exit()
            time.sleep(0.5)

        # 주문 정보 불러오기(_on_chejan_slot은 프로그램 재실행 시, order 딕셔너리에 주문내역이 사라지므로..)
        elif rqname == "opt10075_req":
            self.order = {}

            for i in range(tr_data_cnt):
                code = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "종목코드")
                code_name = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "종목명")
                order_number = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "주문번호") #유일키. 중요하다.
                order_status = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "주문상태") #접수, 확인, 체결
                order_quantity = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "주문수량")
                order_price = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "주문가격")
                current_price = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "현재가")
                order_type = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "주문구분")
                left_quantity = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "미체결수량")
                executed_quantity = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "체결량")
                ordered_time = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "시간")
                fee = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "당일매매수수료")
                tax = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "당일매매세금")

                # 데이터 형변환 및 가공
                code = code.strip()
                code_name = code_name.strip()
                order_number = int(order_number.strip())
                order_status = order_status.strip()
                order_quantity = int(order_quantity.strip())
                order_price = int(order_price.strip())
                current_price = int(current_price.strip().lstrip('+').lstrip('-'))
                order_type = order_type.strip().lstrip('+').lstrip('-')
                left_quantity = int(left_quantity.strip())
                executed_quantity = int(executed_quantity.strip())
                ordered_time = ordered_time.strip()
                fee = int(fee)
                tax = int(tax)

                # code를 키값으로 한 dictionary 변환
                self.order[code] = {
                    '종목코드': code,
                    '종목명': code_name,
                    '주문번호': order_number,
                    '주문상태': order_status,
                    '주문수량': order_quantity,
                    '주문가격': order_price,
                    '현재가': current_price,
                    '주문구분': order_type,
                    '미체결수량': left_quantity,
                    '체결량': executed_quantity,
                    '주문시간': ordered_time,
                    '당일매매수수료': fee,
                    '당일매매세금': tax
                }
            self.tr_data = self.order
            self.tr_event_loop.exit()
            time.sleep(0.5)


        # 포지션 얻어오기(보유종목 관련 정보)
        elif rqname == "opw00018_req":
            self.position = {}

            for i in range(tr_data_cnt):
                code = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "종목번호")
                code_name = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "종목명")
                quantity = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "보유수량")
                purchase_price = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "매입가")
                return_rate = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "수익률(%)")
                current_price = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "현재가")
                total_purchase_price = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "매입금액")
                available_quantity = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "매매가능수량")

                # 받아온 데이터들 형변환
                code = code.strip()[1:]
                code_name = code_name.strip()
                quantity = int(quantity)
                purchase_price = int(purchase_price)
                return_rate = float(return_rate)
                current_price = int(current_price)
                total_purchase_price = int(total_purchase_price)
                available_quantity = int(available_quantity)

                self.position[code] = {
                    "종목명": code_name,
                    "보유수량": quantity,
                    "매입가": purchase_price,
                    "수익률": return_rate,
                    "현재가": current_price,
                    "매입금액": total_purchase_price,
                    "매매가능수량": available_quantity
                }

            self.tr_data = self.position

            self.tr_event_loop.exit()
            time.sleep(0.5)


    # 분봉 가격정보를 요청하는 함수
    def get_price_data(self, code):
        """종목의 상장일부터 가장 최근일자까지의 분봉정보를 가져오는 함수"""
        self.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        self.dynamicCall("SetInputValue(QString, QString)", "틱범위", "1")
        self.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10080_req", "opt10080", 0, "0001")

        time.sleep(0.5)
        print(datetime.datetime.now())
        self.tr_event_loop.exec_()

        ohlcv = self.tr_data

        # while self.has_next_tr_data:
        #     self.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        #     self.dynamicCall("SetInputValue(QString, QString)", "틱범위", "1")
        #     self.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")
        #     self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10080_req", "opt10080", 2, "0001")
        #     self.tr_event_loop.exec_()
        #
        #     for key, val in self.tr_data.items():
        #         ohlcv[key][-1:] = val

        df = pd.DataFrame(ohlcv, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'close_diff',
                                          'A_value', 'B_value', 'F_value', 'G_value', 'D_value', 'I_value', 'VM_1min', 'VM'])

        df['A_value_shift'] = df['A_value'].shift(1)
        df['A_delta'] = df['A_value'] - (df['A_value'].shift(1) * 1.5)
        df['Vindex'] = np.where(((df['D_value'] > df['I_value']) & (df['A_delta'] > 0)) & (df['close_diff'] >= 0) & (df['A_value'].diff(2) > 0), df['high'], 0)

        # # Vindex가 양수인 행만 새로운 데이터프레임으로 저장
        # df1 = df.loc[(df['Vindex'] > 0), ['date', 'Vindex']]

        pd.set_option('display.max_column', 100)
        pd.set_option('display.max_row', 900)

        # print("종목코드", code)
        # print(df)
        # print(df1)

        return df

    # 분봉 가격정보를 요청하는 함수
    def get_price_data_async(self, code):
        """종목의 상장일부터 가장 최근일자까지의 분봉정보를 가져오는 함수"""
        self.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        self.dynamicCall("SetInputValue(QString, QString)", "틱범위", "1")
        self.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10080_req_async", "opt10080", 0, "0001")
        time.sleep(2)
        #self.tr_event_loop.exec_()

        ohlcv = self.tr_data

        # while self.has_next_tr_data:
        #     self.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        #     self.dynamicCall("SetInputValue(QString, QString)", "틱범위", "1")
        #     self.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")
        #     self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10080_req", "opt10080", 2, "0001")
        #     self.tr_event_loop.exec_()
        #
        #     for key, val in self.tr_data.items():
        #         ohlcv[key][-1:] = val

        df = pd.DataFrame(ohlcv, columns=['open', 'high', 'low', 'close', 'volume'], index=ohlcv['date'])
        print("종목코드", code)
        print(df[::-1])
        return df[::-1]    

    # 예수금을 얻어오는 요청 함수
    def get_deposit(self):
        print("get_deposit 호출")
        self.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString)", "비밀번호입력매체구분", "00")
        self.dynamicCall("SetInputValue(QString, QString)", "조회구분", "2")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opw00001_req", "opw00001", 0, "0002")

        self.tr_event_loop.exec_()
        return self.tr_data

    def get_deposit_async(self):
        print("get_deposit_async 호출")
        self.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString)", "비밀번호입력매체구분", "00")
        self.dynamicCall("SetInputValue(QString, QString)", "조회구분", "2")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opw00001_req_async", "opw00001", 0, "0002")
        time.sleep(2)

        # self.tr_event_loop.exec_()
        return self.tr_data


    # 주문 정보 불러오기(_on_chejan_slot은 프로그램 재실행 시, order 딕셔너리에 주문내역이 사라지므로..)
    def get_order_history(self):
        print("get_order_history 호출")
        self.dynamicCall("SetInputValue(QString, QString", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString", "전체종목구분", "0")
        self.dynamicCall("SetInputValue(QString, QString", "체결구분", "0")
        self.dynamicCall("SetInputValue(QString, QString", "매매구분", "0")
        self.dynamicCall("CommRqData(QString, QString, int, QString", "opt10075_req", "opt10075", 0, "0002")
        # time.sleep(5)

        self.tr_event_loop.exec_()
        return self.tr_data

    # 주문 정보 불러오기(_on_chejan_slot은 프로그램 재실행 시, order 딕셔너리에 주문내역이 사라지므로..)
    def get_order_history_async(self):
        print("get_order_history async 호출")
        self.dynamicCall("SetInputValue(QString, QString", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString", "전체종목구분", "0")
        self.dynamicCall("SetInputValue(QString, QString", "체결구분", "0")
        self.dynamicCall("SetInputValue(QString, QString", "매매구분", "0")
        self.dynamicCall("CommRqData(QString, QString, int, QString", "opt10075_req_async", "opt10075", 0, "0002")
        time.sleep(2)

        # self.tr_event_loop.exec_()
        #return self.tr_data    

    # 계좌의 현재 포지션 즉, 계좌평가잔고 내역을 요청하기. 보유종목 불러올 때.
    def get_position(self):
        print("get_position 호출")
        self.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString)", "비밀번호입력매체구분", "00")
        self.dynamicCall("SetInputValue(QString, QString)", "조회구분", "1")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opw00018_req", "opw00018", 0, "0002")
        #time.sleep(5)

        self.tr_event_loop.exec_()
        return self.tr_data

    # 계좌의 현재 포지션 즉, 계좌평가잔고 내역을 요청하기. 보유종목 불러올 때. Async
    def get_position_async(self):
        print("get_position_async 호출")
        self.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString)", "비밀번호입력매체구분", "00")
        self.dynamicCall("SetInputValue(QString, QString)", "조회구분", "1")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opw00018_req_async", "opw00018", 0, "0002")
        #time.sleep(2)

        #self.tr_event_loop.exec_()
        #return self.tr_data    

    # 계좌번호 얻어오는 함수
    def get_account_number(self, tag="ACCNO"):
        account_list = self.dynamicCall("GetLoginInfo(QString)", tag)
        account_number = account_list.split(';')[1]
        print(account_list)
        print(account_number)
        return account_number

    # 시장 내 전체 종목코드들을 얻어오는 함수
    def get_code_list_by_market(self, market_type):
        code_list = self.dynamicCall("GetCodeListByMarket(QString)", market_type)
        print(code_list)
        code_list = code_list.split(';')[:-1]
        return code_list

    # 종목코드에 해당하는 종목명을 반환해주는 함수
    def get_master_code_name(self, code):
        code_name = self.dynamicCall("GetMasterCodeName(QString)", code)
        return code_name

    # 주문을 접수하는 함수
    # order_type >> 1:신규매수, 2:신규매도, 3:매수취소, 4:매도취소, 5:매수정정, 6:매도정정
    def send_order(self, rqname, screen_no, order_type, code, order_quantity, order_price, order_classification, origin_order_number=""):
        """
          LONG nOrderType,  // 주문유형 >> 1:신규매수, 2:신규매도 3:매수취소, 4:매도취소, 5:매수정정, 6:매도정정
          9개 인자값을 가진 국내 주식주문 함수이며 리턴값이 0이면 성공이며 나머지는 에러입니다.
          1초에 5회만 주문가능하며 그 이상 주문요청하면 에러 -308을 리턴합니다.
          모의투자에서는 지정가 주문과 시장가 주문만 가능합니다.
        """
        order_result = self.dynamicCall("SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
            [rqname, screen_no, self.account_number, order_type, code, order_quantity, order_price, order_classification, origin_order_number])

        return order_result

    # 실시간 체결정보 얻어오기(체결가, 매수호가, 매도호가 등 계속 변함)
    def set_real_reg(self, str_screen_no, str_code_list, str_fid_list, str_opt_type):
        """
        :param str_screen_no: 화면번호
        :param str_code_list: 종목코드 리스트 ';'로 묶어서 보냄
        :param str_fid_list: 실시간 FID리스트(어느값이든 하나만 전달하면 다른 값들을 얻어올 수 있음)
        :param str_opt_type: 실시간 등록 타입 >> 0:최초등록, 1:추가등록(최초등록 시 1로 전달해도 무방함)
        :return:
        """
        self.dynamicCall("SetRealReg(QString, QString, QString, QString)", str_screen_no, str_code_list, str_fid_list, str_opt_type)
        print("universe_realtime_transaction_info 요청!!")
        time.sleep(0.5)

    # 조건검색식 조회하기
    def get_condition_load(self):
        result = self.dynamicCall("GetConditionLoad()")
        print("조건검색식 조회 결과 :", result)

        if result == 1:
            print("조건검색식이 올바르게 조회되었습니다.")
        elif result != 1:
            print("조건검색식 조회중 오류가 발생하였습니다.")

        self.condition_loop = QEventLoop()
        self.condition_loop.exec_()

    # 조건검색식 조회하기
    def get_condition_load_async(self):
        result = self.dynamicCall("GetConditionLoad()")
        print("조건검색식 조회 결과 :", result)
        time.sleep(5)

        if result == 1:
            print("조건검색식이 올바르게 조회되었습니다.")
        elif result != 1:
            print("조건검색식 조회중 오류가 발생하였습니다.")

