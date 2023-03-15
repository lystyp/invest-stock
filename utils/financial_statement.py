# -*- coding: UTF-8 -*-
import requests
from io import StringIO
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
import pickle
import datetime
import re
import time
import sqlalchemy
import traceback
from .logging_util import Logger
from sqlalchemy.orm import Session

log = Logger("financial_statement.py")

def requests_get(*args1, **args2):
    i = 3
    while i >= 0:
        try:
            return requests.get(*args1, **args2)
        except (ConnectionError) as error:
            log.e(str(traceback.format_exc()))
            log.e('retry one more time after 60s', i, 'times left')
            time.sleep(60)
        i -= 1
    return pd.DataFrame()

def afterIFRS(year, season):
    season2date = [ datetime.datetime(year, 5, 15),
                    datetime.datetime(year, 8, 14),
                    datetime.datetime(year, 11, 14),
                    datetime.datetime(year+1, 3, 31)]

    return pd.to_datetime(season2date[season-1].date())

# 本來表格的格式是 index : 所有的資料的名稱，像是營收、資產...，columns : stock id
# 這裡把沒用的欄位拿掉，並且轉個方向，把index變成stock id + date，columns變成所有資料的名稱
def clean(year, season, balance_sheet):
    log.d("clean!")
    if len(balance_sheet) == 0:
        log.e('**WARRN: no data to parse')
        return balance_sheet
    balance_sheet = balance_sheet.transpose().reset_index().rename(columns={'index':'stock_id'})

    if '會計項目' in balance_sheet:
        s = balance_sheet['會計項目']
        balance_sheet = balance_sheet.drop('會計項目', axis=1).apply(pd.to_numeric)
        balance_sheet['會計項目'] = s.astype(str)

    balance_sheet['date'] = afterIFRS(year, season)
    
    balance_sheet['stock_id'] = balance_sheet['stock_id'].astype(str)
    balance = balance_sheet.set_index(['stock_id', 'date'])
    return balance

def remove_english(s):
    result = re.sub(r'[a-zA-Z()]', "", s)
    return result

def patch2019(df):
    df = df.copy()
    dfname = df.columns.levels[0][0]

    # 因為第一個欄位只是各種資料的代號，用不到就不理他了
    df = df.iloc[:,1:].rename(columns={'會計項目Accounting Title':'會計項目'})
    
    # 把會計項目那欄的所有字串英文去掉
    refined_name = df[(dfname,'會計項目')].str.split(" ").str[0].str.replace("　", "").apply(remove_english)
    
    subdf = df[dfname].copy()
    subdf['會計項目'] = refined_name
    df[dfname] = subdf
    
    # 把index從level 0是資產負債表Balance Sheet，level 1是下一層變成顛倒過來，資產負債表Balance Sheet變level 1，要幹嘛?
    df.columns = pd.MultiIndex(levels=[df.columns.levels[1], df.columns.levels[0]],codes=[df.columns.codes[1], df.columns.codes[0]])
    def neg(s):
        # 把表格裡的nan換成pd.Nan，數字裡的逗號拿掉，括號表示負數，把括號拿掉負數加上去
        if isinstance(s, float):
            return s
        
        if str(s) == 'nan':
            return np.nan

        s = s.replace(",", "")
        if s[0] == '(':
            return -float(s[1:-1])
        else:
            return float(s)
    # 把資產負載表最上面那欄拿掉
    df.iloc[:,1:] = df.iloc[:,1:].applymap(neg)
    return df

def read_html2019(file):
    dfs = pd.read_html(file)
    return [pd.DataFrame(), patch2019(dfs[0]), patch2019(dfs[1]), patch2019(dfs[2])]
    

def pack_htmls(year, season, directory, save_pickle=True):
    balance_sheet = {}
    income_sheet = {}
    cash_flows = {}
    income_sheet_cumulate = {}
    log.d(os.listdir(directory), "save_pickle = ", save_pickle)
    pbar = tqdm(os.listdir(directory))
    for i in pbar:
        log.d("Load " + str(i))
        # 將檔案路徑建立好
        file = os.path.join(directory, i) 
        
        # 假如檔案不是html結尾，或是太小，代表不是正常的檔案，略過
        if file[-4:] != 'html' or os.stat(file).st_size < 10000:
            log.w("file is too small :" + file)
            continue
        
        # 顯示目前運行的狀況
        stock_id = i.split('.')[0]
        pbar.set_description('parse htmls %d season %d stock %s' % (year, season, stock_id))
        
        # 讀取html，2019年以前跟2019年以後的財報不同
        if year < 2019:
            dfs = pd.read_html(file)
        else:
            try:
                dfs = read_html2019(file)
            except:
                log.e("ERROR** cannot parse ", file, ", error = ", str(traceback.format_exc()))
                continue

        # 處理pandas0.24.1以上，會把columns parse好的問題
        for df in dfs:
            if 'levels' in dir(df.columns):
                df.columns = list(range(df.values.shape[1]))#list(range(max_col))
        
        # 假如html不完整，則略過
        if len(dfs) < 4:
            log.e('**WARRN html file broken', year, season, i)
            continue
        
        # dfs[0]在2019之後的財報的話會是read_html2019產生的empty，不知道要幹嘛，反正用不到
        # 取得 balance sheet，其中df.index是欄位名稱、df.columns就都是數值，column有的有兩欄有的有三欄，名稱會是[1,2,...]
        df = dfs[1].copy().drop_duplicates(subset=0, keep='last')
        df = df.set_index(0)
        # 就是取第0欄，也就是df[1]
        balance_sheet[stock_id] = df[1].dropna()
        #balance_sheet = combine(balance_sheet, df[1].dropna(), stock_id)

        # 取得 income statement
        df = dfs[2].copy().drop_duplicates(subset=0, keep='last')
        df = df.set_index(0)
        # 每個財報不同，第二季跟第三季四欄，第一季跟第四季只有兩欄不知為何，第一季的單季資料就跟累積的資料一樣，
        # 第四季的單季資料後面會在用fill_season4抓Q4累積-Q3累積來算出來
        # 假如有4個columns，則第0與第2條column，就是名稱1與名稱3，是單季跟累計的income statement，第二跟第四欄就是去年的表現
        if len(df.columns) == 4:
            income_sheet[stock_id] = df[1].dropna()
            income_sheet_cumulate[stock_id] = df[3].dropna()
        # 假如有2個columns，則代表第0條column，就是名稱1的那條，為累計的income statement，單季的從缺
        elif len(df.columns) == 2:
            income_sheet_cumulate[stock_id] = df[1].dropna()
            
            # 假如是第一季財報 累計 跟單季 的數值是一樣的
            if season == 1:
                income_sheet[stock_id] = df[1].dropna()

        # 取得 cash_flows
        df = dfs[3].copy().drop_duplicates(subset=0, keep='last')
        df = df.set_index(0)
        cash_flows[stock_id] = df[1].dropna()
    # 將dictionary整理成dataframe
    balance_sheet = pd.DataFrame(balance_sheet)
    income_sheet = pd.DataFrame(income_sheet)
    income_sheet_cumulate = pd.DataFrame(income_sheet_cumulate)
    cash_flows = pd.DataFrame(cash_flows)
    
    # 做清理
    ret = {'balance_sheet':clean(year, season, balance_sheet), 'income_sheet':clean(year, season, income_sheet), 
            'income_sheet_cumulate':clean(year, season, income_sheet_cumulate), 'cash_flows':clean(year, season, cash_flows)}
    
    # 假如是第一季的話，則 單季 跟 累計 是一樣的
    if season == 1:
        ret['income_sheet'] = ret['income_sheet_cumulate'].copy()

    ret['income_sheet_cumulate'].columns = '累計' + ret['income_sheet_cumulate'].columns
    if save_pickle:
        pickle.dump(ret, open('data/financial_statement/pack' + str(year) + str(season) + '.pickle', 'wb'))
    
    return ret

# 讀所有pickles，以季為單位，回傳dict，key是年分+季，value是另一個dict，
# 內容是{表格名稱 : 資料}，表格名稱就是income_sheet、cash_flows那些
def get_all_pickles(directory):
    ret = {}
    for i in os.listdir(directory):
        if i[:4] != 'pack':
            continue
        ret[i[4:9]] = pd.read_pickle(os.path.join(directory, i))
        #ret[i[4:9]] = pickle.load(open(os.path.join(directory, i), 'rb'))
    return ret

# 把透過pickle讀進來的N季的財報都塞到同一個dataframe裡面，index已經有用stock id跟date排好了所以不用擔心
def combine(d):
    tnames = ['balance_sheet',
            'cash_flows',
            'income_sheet',
            'income_sheet_cumulate']

    tbs = {t:pd.DataFrame() for t in tnames}

    for i, dfs in d.items():
        for tname in tnames:
            tbs[tname] = tbs[tname].append(dfs[tname])
    return tbs

   
def fill_season4(tbs):
    # copy income sheet (will modify it later)
    income_sheet = tbs['income_sheet'].copy()
    # calculate the overlap columns
    c1 = set(tbs['income_sheet'].columns)
    c2 = set(tbs['income_sheet_cumulate'].columns)
    
    # 如果是load歷年所有pickle資料進來的話，基本上不用求overlap_columns，因為c1就是c2的子集合而已(可能是剛好)
    overlap_columns = []
    for i in c1:
        if '累計' + i in c2:
            overlap_columns.append('累計' + i)

    # get all years，levels[1]應該是date吧我猜，level 0 是stock id
    years = set(tbs['income_sheet_cumulate'].index.levels[1].year)
    for y in years:
        # get rows of the dataframe that is season 4
        ys = tbs['income_sheet_cumulate'].reset_index('stock_id').index.year == y # 取第Y年的資料
        ds4 = tbs['income_sheet_cumulate'].reset_index('stock_id').index.month == 3 # 取所有三月的資料
        # 把去年Q4的資料轉成數字，去年Q4今年三月拿到，並且index剩下stock id
        df4 = tbs['income_sheet_cumulate'][ds4 & ys].apply(lambda s: pd.to_numeric(s, errors='coerce')).reset_index('date') 
        
        # get rows of the dataframe that is season 3，跟上面一樣，把去年Q3轉成數字
        yps = tbs['income_sheet_cumulate'].reset_index('stock_id').index.year == y - 1
        ds3 = tbs['income_sheet_cumulate'].reset_index('stock_id').index.month == 11
        df3 = tbs['income_sheet_cumulate'][ds3 & yps].apply(lambda s: pd.to_numeric(s, errors='coerce')).reset_index('date')

        if len(df3) == 0:
            log.e('income_sheet Q4 data in ', y-1, " might be empty , cause we can't find Q3 data in income_sheet_cumulate.")
            continue
        
        # calculate the differences of income_sheet_cumulate to get income_sheet single season
        diff = df4 - df3
        diff = diff.drop(['date'], axis=1)[overlap_columns]
        # remove 累計
        diff.columns = diff.columns.str[2:]
        
        # 加上第四季的日期
        diff['date'] = pd.to_datetime(str(y) + '-03-31')
        diff = diff[list(c1) + ['date']].reset_index().set_index(['stock_id','date'])
        
        # 新增資料於income_sheet尾部
        income_sheet = income_sheet.append(diff)
        
    # 排序好並更新tbs
    # 如果income_sheet_cumulate是第四季資料，但是往回找找不到income_sheet_cumulate第三季資料的話，
    # 沒辦法算出第四季的income_sheet，income_sheet會是空的
    if not income_sheet.empty:
        income_sheet = income_sheet.reset_index().sort_values(['stock_id', 'date']).set_index(['stock_id', 'date'])
    tbs['income_sheet'] = income_sheet

def caculate_income_sheet_Q4(tbs, income_sheet_cumulate_Q3):
    income_sheet_cumulate_Q4 = tbs['income_sheet_cumulate']
    if income_sheet_cumulate_Q4.empty:
        log.e('income_sheet_cumulate_Q4 is empty, something wrong.')
        return
    
    year = tbs['income_sheet_cumulate'].reset_index('stock_id').index.year[0]
    month = tbs['income_sheet_cumulate'].reset_index('stock_id').index.month[0]
    log.d('caculate_income_sheet_Q4, year = ', year, ", month = ", month)

    income_sheet = tbs['income_sheet'].copy()
    if not income_sheet.empty:
        log.e('income_sheet for Q4 is not empty, something wrong.')
        return
    if month != 3:
        log.e('income_sheet_cumulate data is in ', month, ', not Q4 data.')
        return
    if len(income_sheet_cumulate_Q3) == 0:
        log.e('income_sheet Q4 data in ', year - 1, " might be empty , cause we can't find Q3 data in income_sheet_cumulate.")
        return
    

    # calculate the overlap columns，每一季給一間公司提供的財報都會多少有點不同，所以欄位都不太相同，這裡以要求的Q4為主
    col_Q3 = set(income_sheet_cumulate_Q3.columns)
    col_Q4 = set(tbs['income_sheet_cumulate'].columns)
    
    overlap_columns = []
    for i in col_Q4:
        if i in col_Q3:
            overlap_columns.append(i)

    # 把去年Q4的資料轉成數字(應為是html抓出來的，可能還是字串吧我猜)，去年Q4今年三月拿到，並且index剩下stock id
    # 如果沒有to_numeric會在df.to_sql的時候發生 row too long的問題，不知是啥
    income_sheet_cumulate_Q3 = income_sheet_cumulate_Q3.apply(lambda s: pd.to_numeric(s, errors='coerce')).reset_index('date') 
    income_sheet_cumulate_Q4 = income_sheet_cumulate_Q4.apply(lambda s: pd.to_numeric(s, errors='coerce')).reset_index('date') 
    

    # calculate the differences of income_sheet_cumulate to get income_sheet single season
    diff = income_sheet_cumulate_Q4 - income_sheet_cumulate_Q3
    diff = diff.drop(['date'], axis=1)[overlap_columns]
    # 加上第四季的日期
    diff['date'] = pd.to_datetime(str(year) + '-03-31')
    diff = diff[overlap_columns + ['date']].reset_index().set_index(['stock_id','date'])
    # remove 累計
    diff.columns = diff.columns.str[2:]
    # 新增資料於income_sheet尾部，income_sheet所擁有的columns只包含Q3資料庫、Q4html兩邊都有的資料
    income_sheet = income_sheet.append(diff)
    
    # 更新tbs
    income_sheet = income_sheet.reset_index().sort_values(['stock_id', 'date']).set_index(['stock_id', 'date'])
    tbs['income_sheet'] = income_sheet

def replace_to_db(conn, tbs):
    log.d('save table to db')
    for i, df in tbs.items():
        if df.empty:
            log.e(i, " table is empty.")
        else:
            df = df.reset_index().sort_values(['stock_id', 'date']).drop_duplicates(['stock_id', 'date']).set_index(['stock_id', 'date'])
            df = df[df.count().nlargest(900).index]
            log.d('save table ', i, " , size = ", df.shape)
            df.to_sql(i, conn.engine, if_exists='replace', dtype={'stock_id':sqlalchemy.types.VARCHAR(30)})
            log.d(i, " save to db successly.")

def merge_to_sql(conn, name, df):
# get the existing dataframe in database
    exist = table_exist(conn, name)
    log.d(name, " merge_to_sql, original table is exist :" + str(exist))
    df = df.copy()
    df = df.reset_index().sort_values(['stock_id', 'date']).drop_duplicates(['stock_id', 'date']).set_index(['stock_id', 'date'])
    log.d('save table ', name, " , size = ", df.shape)
    try:
        # 這裡有大bug啦!!! conn參數不是丟connection，是丟engine，否則table會是空的(不知道是不是只有MySQL會這樣)
        # https://stackoverflow.com/questions/48307008/pandas-to-sql-doesnt-insert-any-data-in-my-table

        # 要存回MySQL因為string型態不能當index及key，所以要把string改成varchar
        if exist:
            # 要先把要新增的ret存到一個暫存的temp table，接著再透過mySQL指令把temp合併到目標資料夾裡，若資料已存在就update，不存在就insert
            # 因此我們要先把stock_id跟date合併成一組unique的index，後續的ON DUPLICATE KEY UPDATE 指令才可以根據index判斷有沒有重複
            # 所以我先檢查我之前有把有把index設成unique過，沒有就設，有就不用再設定一次了
            is_index_not_unique = pd.read_sql(sqlalchemy.text('SHOW INDEX FROM ' + name + ' where Key_name = \'UQ_stock_id_date\';'), conn).empty
            if is_index_not_unique:
                log.d("Table index is not unique, set it to unique.") 
                cmd = 'ALTER TABLE ' + name + ' ADD CONSTRAINT UQ_stock_id_date UNIQUE (`stock_id`, `date`);'
                conn.execute(sqlalchemy.text(cmd))

            # 可能會有一個問題是temp table有新的columns是原table沒有的，要幫原table新增一下，但又會衍生另外一個問題是，
            # sql的每個row有大小限制，又我每期財報都有不一樣的欄位，那很容易就新增到超過上限了
            # finlab的作法一次把往年所有的財報用pickle讀進來，這樣dataframe就已經有所有的columns了，再把values最多的前九百個columns存進sql
            # 但這樣跟我想要一季一季存的作法背道而馳，所以我決定先以finlab提供的table裡有的columns為基準，只新增sql裡的table裡有出現的columns
            # 反正我有存pickles跟html檔，之後有問題再說
            original_col_df = pd.read_sql(sqlalchemy.text('show columns from `' + name + '`;'), conn)
            original_col_list = list(original_col_df["Field"])
            new_col_list = list(df.columns)
            duplicate_col_list = []
            dropped_col_list = []
            for col in new_col_list:
                if col in original_col_list:
                    duplicate_col_list.append(col)
                else:
                    dropped_col_list.append(col)
            df = df[pd.Index(duplicate_col_list)]
            log.d("duplicate_col_list size = ", len(duplicate_col_list), ", ", len(new_col_list) - len(duplicate_col_list), " columns are dropped.")
            log.d(name, " , new size = ", df.shape)
            log.d("Dropped col list : ", dropped_col_list)

            # 存一個暫用的temp table等等合併要用
            temp_table = name + "_temp" 
            log.d("Save df to temp table :" +   temp_table)
            df.to_sql(temp_table, conn.engine, if_exists='replace', dtype={'stock_id':sqlalchemy.types.VARCHAR(30)})
            

            # 把temp表格merge回原本的表格後再砍掉temp
            s1 = '`stock_id`, `date`' # 取得index name
            s2 = ""
            for i in range(len(df.columns)):
                s1 += ", `" + df.columns[i] + "`" 
                s2 += ", `" + df.columns[i] + "` = VALUES(`" + df.columns[i] + "`)"
            s2 = s2[1:] # 去掉第一個逗號
            cmd = 'INSERT INTO `' + name + '`(' + s1 + ')' + ' SELECT * FROM `' + temp_table + '` ON DUPLICATE KEY UPDATE ' + s2 + ';'
            # cmd = 'REPLACE INTO `' + name + '`(' + s1 + ')' + ' SELECT * FROM `temp`;'
            log.d("Insert table ", name, " with ON DUPLICATE KEY UPDATE.") 
            # 更動表格資料的相關操作需要commit，像是插入、更新、刪除列之類的
            # 如果用conn.execute會出現error
            # 'Table definition has changed, please retry transaction'，不知為何
            session = Session(conn.engine)
            session.execute(sqlalchemy.text(cmd))
            session.commit()
            conn.execute(sqlalchemy.text('DROP TABLE `' + temp_table + '`;'))
        else:
            df.to_sql(name, conn.engine, if_exists='replace', dtype={'stock_id':sqlalchemy.types.VARCHAR(30)})
        log.d(name, " save to db successly.")
    except Exception:
        log.e("Error : " + str(traceback.format_exc()))

def table_exist(conn, table):
    cursor = conn.execute(sqlalchemy.text("SHOW TABLES LIKE '" + table + "';"))
    return len(cursor.fetchall()) > 0

def html2db(conn, date):
    year = date.year
    if date.month == 3:
        season = 4
        year = year - 1
        month = 11
    elif date.month == 5:
        season = 1
        month = 2
    elif date.month == 8:
        season = 2
        month = 5
    elif date.month == 11:
        season = 3
        month = 8
    else:
        return None
    
    # 把當年度下載下來的html全部轉DataFrame之後存成pickle
    pack_htmls(year, season, os.path.join('data', 'financial_statement', str(year) + str(season)))
    # 讀往年所有的pickles，再一起用replace的方式存進DB
    d = get_all_pickles(os.path.join('data', 'financial_statement'))
    tbs = combine(d)
    fill_season4(tbs)
    # 最後存進DB
    replace_to_db(conn, tbs)

# 原本的html2db是把歷年所有pickles都讀進來，接著再重新塞進資料庫，我現在不這麼做了，我只會塞當下那一年append進資料庫，
# 所以我也不用pickles了
def html2db_single_season(conn, date):
    year = date.year
    if date.month == 3:
        season = 4
        year = year - 1
    elif date.month == 5:
        season = 1
    elif date.month == 8:
        season = 2
    elif date.month == 11:
        season = 3
    else:
        log.d("Month ", date.month, " does not release financial report.")
        return None
    
    # 把html資料轉成dataframe，下面會回傳一個dict，key就是財報的四個表格的名稱，value就是dataframe
    dfs = pack_htmls(year, season, os.path.join('data', 'financial_statement', str(year) + str(season)))
    # 這行測試用的，dfs = pd.read_pickle(os.path.join('/home/ec2-user/invest-stock/data/financial_statement/', 'pack' + str(year) + str(season) + '.pickle'))
    # 第1~3季的income_sheet都在pack_htmls就可以抓到，但第四季的只能另外算
    if season == 4:
        cmd = 'SELECT * FROM income_sheet_cumulate WHERE YEAR(`date`) = \'' + \
            str(date.year -1) + '\' and MONTH(`date`) = 11;'
        log.d("select income_sheet_cumulate_Q3 data, cmd :", cmd)
        income_sheet_cumulate_Q3 = pd.read_sql(sqlalchemy.text(cmd), conn, index_col=['stock_id', 'date'])
        caculate_income_sheet_Q4(dfs, income_sheet_cumulate_Q3)


    # 最後存進DB
    log.d('save tables to db')
    for i, df in dfs.items():
        if df.empty:
            log.e(i, " table is empty.")
        else:
            merge_to_sql(conn, i, df)