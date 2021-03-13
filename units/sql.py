import pymysql
import settings


class StockSQL:
    def __init__(self):
        host = settings.HOST
        port = settings.PORT
        user = settings.USER
        password = settings.PASSWORD
        dbname = settings.DBNAME
        charset = settings.CHARSET
        # 開啟資料庫連線
        self.db = pymysql.connect(host=host, port=port, user=user, password=password, db=dbname, charset=charset)

    def read_one_line(self, sql):
        """

        :param sql: "SELECT max(日期) FROM AI分析;"
        :return:
        """
        # 使用 cursor() 方法建立遊標物件 cursor
        with self.db.cursor() as cursor:
            # 使用 execute() 方法执行SQL查询
            cursor.execute(sql)
            # 使用 fetchone() 方法获取单条数据
            data = cursor.fetchone()
        return data

    def read_multi_line(self, sql):
        # 使用 cursor() 方法建立遊標物件 cursor
        with self.db.cursor() as cursor:
            # 使用 execute() 方法执行SQL查询
            cursor.execute(sql)
            # 使用 fetchall() 方法获取多条数据
            data = cursor.fetchall()
        return data

    def read_1723_stock_ids(self):
        """

        :return: type tuple(tuple(str)), (('1101'), ('2312'), ...)
        """
        select = "SELECT C.代號A \
                      FROM ( \
                            select A.代號A,B.代號 AS 代號B \
                            from (SELECT substr(代號名稱, 1, 4) as 代號A FROM base__股票代號) A \
                            LEFT JOIN base__下市股票 B ON A.代號A = B.代號 \
                            ) C \
                      WHERE C.代號B IS NULL \
                      ORDER BY C.代號A \
                      ;"
        # 使用 cursor() 方法建立遊標物件 cursor
        with self.db.cursor() as cursor:
            cursor.execute(select)
            result = cursor.fetchall()
        return result

    @staticmethod
    def read_171_stock_ids():
        """

        :return: type tuple(tuple(str)), (('1101'), ('2312'), ...)
        """
        import sqlite3
        import pandas as pd

        con = sqlite3.connect("dataset/Stock_Database.sqlite")
        data = pd.read_sql_query("SELECT * FROM Stock_No", con)
        stock_ids = data['StockNo']
        return stock_ids

    def read_stock_values(self, select):
        """
        :param select = "SELECT 日期, 開, 高, 低, 收, MA20, K9, D9, OSC, UB20, PB, BW, 成交量 \
                      FROM StockAll \
                      WHERE 代號 = '" + stock_id + "' \
                      ORDER BY 日期 DESC \
                      LIMIT 4000 \
                      ;"
        :return:
        """
        # 使用 cursor() 方法建立遊標物件 cursor
        with self.db.cursor() as cursor:
            cursor.execute(select)
            result = cursor.fetchall()
        return result

    def read_stock_values_and_pred(self, stock_id):
        """
                 [   0,       1,         2,  3,   4,    5,  6,  7,  8,  9,    10,   11,   12,    13,     14,      15,   16]
            Get: [日期, 每日日線, 信心指數a, id, 代號, 日期, 開, 高, 低, 收, 成交量, 漲跌, Week, Month, 漲跌幅, 成交金額, Year]
                 SELECT DISTINCT A.日期,C.每日日線,B.信心指數 as 信心指數a,A.*
                 [   0,              1,        2,               3,               4,        5,  6,    7,   8,  9, 10, 11, 12,    13,   14,   15,    16,     17,      18,   19]
                 [日期, 每日周線談整突破, 每日日線, 每日日線盤整突破, 每日日線布林條件, 信心指數a, id, 代號, 日期, 開, 高, 低, 收, 成交量, 漲跌, Week, Month, 漲跌幅, 成交金額, Year]
                 SELECT DISTINCT A.日期,C.每日週線盤整突破,C.每日日線,C.每日日線盤整突破,C.每日日線布林條件,B.信心指數 as 信心指數a,A.*
        """
        select = "SELECT DISTINCT A.日期,C.每日週線盤整突破,C.每日日線,C.每日日線盤整突破,C.每日日線布林條件,B.信心指數 as 信心指數a,A.* \
                  FROM Stock.技術面__每日線圖 A \
                  LEFT OUTER JOIN Stock.AI__分析a B ON B.代號 = A.代號 AND B.日期 = A.日期 \
                  LEFT OUTER JOIN Stock.技術面__每日技術線 C ON C.代號 = A.代號 AND C.日期 = A.日期 \
                  WHERE A.代號 = {} \
                  AND A.日期 >= '2020-01-01' \
                  ORDER BY A.日期 ASC".format(stock_id)
        with self.db.cursor() as cursor:
            cursor.execute(select)
            result = cursor.fetchall()

        category_data = []
        values = []
        volumes = []
        other1_markpoint_coord, other2_markpoint_coord, other3_markpoint_coord, other4_markpoint_coord = [], [], [], []
        other1_markpoint_value, other2_markpoint_value, other3_markpoint_value, other4_markpoint_value = [], [], [], []
        ai_markpoint_coord = []
        ai_markpoint_value = []
        for i, r in enumerate(result):
            date, op, cl, lowest, highest, volume = str(r[0]), r[9], r[12], r[11], r[10], r[13]
            other_result1, other_result2, other_result3, other_result4, ai_result = r[1], r[2], r[3], r[4], r[5]
            category_data.append(date)
            values.append([date, op, cl, lowest, highest, volume])
            volumes.append([i, highest, 1 if op > cl else -1])
            count = 0
            if ai_result:
                if ai_result > 0.5:
                    ai_markpoint_coord.append([date, highest])
                    ai_markpoint_value.append(int(ai_result*100))
                    count += 0.5
            if other_result1:
                if other_result1 == '1':
                    other1_markpoint_coord.append([date, highest+count])
                    other1_markpoint_value.append('+')
                    count += 0.5
            if other_result2:
                if other_result2 == '1':
                    other2_markpoint_coord.append([date, highest+count])
                    other2_markpoint_value.append('+')
                    count += 0.5
            if other_result3:
                if other_result3 == '1':
                    other3_markpoint_coord.append([date, highest+count])
                    other3_markpoint_value.append('+')
                    count += 0.5
            if other_result4:
                if other_result4 == '1':
                    other4_markpoint_coord.append([date, highest+count])
                    other4_markpoint_value.append('+')
                    count += 0.5
        return {
            "categoryData": category_data,
            "values": values,
            "volumes": volumes,
            "other1_markpoint_coord": other1_markpoint_coord,
            "other1_markpoint_value": other1_markpoint_value,
            "other2_markpoint_coord": other2_markpoint_coord,
            "other2_markpoint_value": other2_markpoint_value,
            "other3_markpoint_coord": other3_markpoint_coord,
            "other3_markpoint_value": other3_markpoint_value,
            "other4_markpoint_coord": other4_markpoint_coord,
            "other4_markpoint_value": other4_markpoint_value,
            "ai_markpoint_coord": ai_markpoint_coord,
            "ai_markpoint_value": ai_markpoint_value,
        }


if __name__ == "__main__":
    stock_sql = StockSQL()
    # stock_sql.write_multi_line(
    #     "INSERT INTO `AI分析` VALUES (%s,%s,%s)",
    #     ret=[
    #         ('2020-12-22', '2330', '0.5458214'),
    #         ('2020-12-22', '2330', '0.5458214'),
    #         ('2020-12-22', '2330', '0.5458214')
    #     ]
    # )
    r = stock_sql.read_stock_values_and_pred(3005)
    print(r)

    # 讀取Data
    # stock_ids = stock_sql.read_stock_ids()
    # v = stock_sql.read_stock_values(stock_ids[0][0])
    # print(v)
    print()
