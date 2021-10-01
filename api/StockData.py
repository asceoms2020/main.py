class StockData:
    def __init__(self):
        print("StockData Model")
        #self.kiwoom = Kiwoom()
        #변수선언
        self.__universe = {}
        self.__deposit = 0

    # getter
    def getUniverse(self):
        return self.__universe

    # setter
    def setUniverse(self, code, code_name):
        self.__universe[code] = {
            "code_name": code_name
        }

    # setter
    def setPrice(self, code, price_df):
        self.__universe[code]["price_df"] = price_df
