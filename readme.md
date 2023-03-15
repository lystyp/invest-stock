Todo:
每個策略自己一份data，initial會很久
===============================================
爬蟲抓資料下來只要更新抓下來那一筆就好，不用整個資table蓋掉
===============================================
to_sql要不要加上chunk限制
===============================================
Q4的income_sheet是從Q4-Q3的累積income_sheeet來的，如果沒有Q3累積資料的話要怎麼處裡Q4 income_sheet?留空還是跳過?先留空吧
===============================================
有2018/03/31的cash_flows是空的只有兩支股票有資料，記得爬蟲補上，自己實際上去找是有資料的，應該只是爬蟲時fail了
    import pandas as pd
    import utils.financial_statement as fin
    d = pd.read_pickle("./pack20174.pickle")
    fin.merge_to_sql(db_util.get_db_connection(),"cash_flows", d["cash_flows"])
===============================================
在存財報進db的時候，他只取最多資料的前900 columns，
df[df.count().nlargest(900).index] 是因為如果column太多，pandas在轉sql指令的時候就會因為一行row的容量超過上限exception了，
如果我to_sql的方法用 method='multi'，一樣也會跳出exception，有夠麻煩
===============================================
因為財報每一期提供的資料都不一定相同，又sql的每一個row都有容量上限，我不可能把一兩千個欄位都存下來，
finlab的做法是把全部的pickle都讀進dataframe裡，然後計算資料最多的前九百個欄位，
但我不想要讀全部pickle這種超耗時間的做法，我就直接沿用sql裡現有的欄位，
我爬下來的最新財報去跟sql裡的財報欄位做比較，有出現在sql裡的欄位我才存
用to_sql太慢時，可以考慮用to_csv存csv，快很多，再用sql指令去load csv檔案也可以
===============================================
Windows系統不支援log file切換，所以log會無限制增長下去
===============================================
sql連線需要關嗎?
https://stackoverflow.com/questions/8645250/how-to-close-sqlalchemy-connection-in-mysql
sqlalchemy好像有connect pool的設置，沒問題就先不理它吧
===============================================
mySQL的insert操作用conn.execute會發生error : # 'Table definition has changed, please retry transaction'，不知為何
改成用session.execute加上commit就可以了.....目前還不知道原因
