from api.Kiwoom import *
from strategy.RSIStrategy import *
import sys

app = QApplication(sys.argv)
# kiwoom = Kiwoom()
# kiwoom.get_account_number()

# kospi_code_list = kiwoom.get_code_list_by_market("0")
# for code in kospi_code_list:
#     code_name = kiwoom.get_master_code_name(code)
#     print(code, code_name)
#
# kosdaq_code_list = kiwoom.get_code_list_by_market("10")
# for code in kosdaq_code_list:
#     code_name = kiwoom.get_master_code_name(code)
#     print(code, code_name)

# kiwoom.get_price_data("078130")

# deposit = kiwoom.get_deposit()

# order_result = kiwoom.send_order('send_buy_order', '1001', 1, '007700', 1, 34200, '00')
# print(order_result)

# order_history = kiwoom.get_order_history()
# print(order_history)

# position = kiwoom.get_position()
# print("현재 보유중인 종목들 : ", position)

# fids = get_fid("체결시간")
# codes = "007700;"
# kiwoom.set_real_reg("1000", codes, fids, "0")

#kiwoom.get_condition_load()

rsi_strategy = RSIStrategy()
rsi_strategy.thread_run()

app.exec_()  #app.exec_() 덕분에 프로그램이 종료되지 않고 계속 실행될 수 있다.