#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import concurrent.futures
import os.path
import sys
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.stockfetch as stf

__author__ = 'myh '
__date__ = '2023/3/10 '

# 设置起始日期
start_date = datetime(2020, 1, 1)
# 设置结束日期（注意：这里不包含结束日期本身，如果需要包含，则循环条件需要调整）
end_date = datetime(2024, 12, 31)
# 设置时间间隔为6个月
interval = relativedelta(months=6)

# 每日股票龙虎榜
def save_nph_stock_top_data(date, before=True):
    if before:
        return

    try:
        data = stf.fetch_stock_top_data(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_TOP['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_TOP['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_stock_top_data处理异常：{e}")
    stock_spot_buy(date)


# 每日股票资金流向
def save_nph_stock_fund_flow_data(date, before=True):
    if before:
        return

    try:
        times = tuple(range(4))
        results = run_check_stock_fund_flow(times)
        if results is None:
            return

        for t in times:
            if t == 0:
                data = results.get(t)
            else:
                r = results.get(t)
                if r is not None:
                    r.drop(columns=['name', 'new_price'], inplace=True)
                    data = pd.merge(data, r, on=['code'], how='left')

        if data is None or len(data.index) == 0:
            return

        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        table_name = tbs.TABLE_CN_STOCK_FUND_FLOW['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_FUND_FLOW['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_nph_stock_fund_flow_data处理异常：{e}")


def run_check_stock_fund_flow(times):
    data = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(times)) as executor:
            future_to_data = {executor.submit(stf.fetch_stocks_fund_flow, k): k for k in times}
            for future in concurrent.futures.as_completed(future_to_data):
                _time = future_to_data[future]
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[_time] = _data_
                except Exception as e:
                    logging.error(f"basic_data_other_daily_job.run_check_stock_fund_flow处理异常：代码{e}")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.run_check_stock_fund_flow处理异常：{e}")
    if not data:
        return None
    else:
        return data


# 每日行业资金流向
def save_nph_stock_sector_fund_flow_data(date, before=True):
    if before:
        return

    # times = tuple(range(2))
    # with concurrent.futures.ThreadPoolExecutor(max_workers=len(times)) as executor:
    #     {executor.submit(stock_sector_fund_flow_data, date, k): k for k in times}
    stock_sector_fund_flow_data(date, 0)
    stock_sector_fund_flow_data(date, 1)

def stock_sector_fund_flow_data(date, index_sector):
    try:
        times = tuple(range(3))
        results = run_check_stock_sector_fund_flow(index_sector, times)
        if results is None:
            return

        for t in times:
            if t == 0:
                data = results.get(t)
            else:
                r = results.get(t)
                if r is not None:
                    data = pd.merge(data, r, on=['name'], how='left')

        if data is None or len(data.index) == 0:
            return

        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        if index_sector == 0:
            tbs_table = tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY
        else:
            tbs_table = tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT
        table_name = tbs_table['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs_table['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`name`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_sector_fund_flow_data处理异常：{e}")


def run_check_stock_sector_fund_flow(index_sector, times):
    data = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(times)) as executor:
            future_to_data = {executor.submit(stf.fetch_stocks_sector_fund_flow, index_sector, k): k for k in times}
            for future in concurrent.futures.as_completed(future_to_data):
                _time = future_to_data[future]
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[_time] = _data_
                except Exception as e:
                    logging.error(f"basic_data_other_daily_job.run_check_stock_sector_fund_flow处理异常：代码{e}")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.run_check_stock_sector_fund_flow处理异常：{e}")
    if not data:
        return None
    else:
        return data



# 每日股票分红配送
def save_nph_stock_bonus(date, before=True):
    print(f"save_nph_stock_bonus:{date}")
   
    
    if before:
        return

    try:
        data = stf.fetch_stocks_bonus(date,False)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_BONUS['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            try:
                columns = tbs.TABLE_CN_STOCK_BONUS['columns']
                cols_type = tbs.get_field_types(columns)
                print(cols_type)  # 打印列的数据类型
                print(cols_type)  # 打印列的数据类型
            except Exception as e:
                print(f"An error occurred: {e}")


           # cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_BONUS['columns'])
            

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_nph_stock_bonus处理异常：{e}")


# 基本面选股
def stock_spot_buy(date):
    try:
        _table_name = tbs.TABLE_CN_STOCK_SPOT['name']
        if not mdb.checkTableIsExist(_table_name):
            return

        sql = f'''SELECT * FROM `{_table_name}` WHERE `date` = '{date}' and 
                `pe9` > 0 and `pe9` <= 20 and `pbnewmrq` <= 10 and `roe_weight` >= 15'''
        data = pd.read_sql(sql=sql, con=mdb.engine())
        data = data.drop_duplicates(subset="code", keep="last")
        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_SPOT_BUY['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SPOT_BUY['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_spot_buy处理异常：{e}")

# 往日股票分红配送
def save_nph_stock_bonus1(start_date, end_date, interval_months=3):
    # 将日期转换为 datetime 对象（如果它们不是的话）
    #start_date = datetime.strptime(start_date, "%Y-%m-%d")
    #end_date = datetime.strptime(end_date, "%Y-%m-%d")
    # 设置时间间隔
    interval = timedelta(days=interval_months * 30.44)  # 注意：这只是一个近似值，因为月份天数不同
    # 或者使用 dateutil.relativedelta 进行更精确的月份计算
    # from dateutil.relativedelta import relativedelta
    # interval = relativedelta(months=interval_months)
 
    current_date = start_date
    current_date = current_date.date()
    end_date = end_date.date()
    while current_date <= end_date:
        try:
            data = stf.fetch_stocks_bonus(current_date,True)  # 假设 fetch_stocks_bonus 需要 date 对象
            if data is None or data.empty:
                continue  # 如果数据为空，则跳过当前循环迭代
 
            table_name = tbs.TABLE_CN_STOCK_BONUS['name']
            # 检查表是否存在
            if mdb.checkTableIsExist(table_name):
                # 删除旧数据
                del_sql = f"DELETE FROM `{table_name}` WHERE `date` = '{current_date}'"
                mdb.executeSql(del_sql)
                cols_type = None
            # 如果表不存在，则获取列类型（这部分逻辑可能需要根据实际情况调整）
            else:
                try:
                    columns = tbs.TABLE_CN_STOCK_BONUS['columns']
                    cols_type = tbs.get_field_types(columns)
                    print(cols_type)  # 打印列的数据类型
                    print(cols_type)  # 打印列的数据类型
                except Exception as e:
                    print(f"An error occurred: {e}")
                #cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_BONUS['columns'])
 
            # 将数据插入数据库
            ccdate = datetime.now().strftime("%Y-%m-%d")
            mdb.insert_db_from_df(data, table_name, cols_type, False, "`plan_date`,`code`")
            #mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
 
        except Exception as e:
            logging.error(f"save_nph_stock_bonus处理异常：{e}")
 
        # 增加时间间隔
        # 使用 timedelta 的方法（可能需要调整以匹配实际的月份间隔）
        # current_date += interval
        # 或者使用 dateutil.relativedelta 进行更精确的月份计算
        current_date += relativedelta(months=interval_months)  # 如果选择了使用 relativedelta

   
    


    


def main():
    runt.run_with_args(save_nph_stock_top_data)
    runt.run_with_args(save_nph_stock_bonus)
    runt.run_with_args(save_nph_stock_fund_flow_data)
    runt.run_with_args(save_nph_stock_sector_fund_flow_data)
    save_nph_stock_bonus1(start_date, end_date, interval_months=3)


# main函数入口
if __name__ == '__main__':
    main()
