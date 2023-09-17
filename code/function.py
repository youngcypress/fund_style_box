import pandas as pd
import pandas_market_calendars as mcal
import datetime
from concurrent.futures import ProcessPoolExecutor

def generate_quarter_dates(start_date, end_date, method):
    # 将字符串转化为日期对象
    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)

    # 创建一个空的结果列表
    result = []
    if method == 'quarter':
        # 找到起始日期的下一个季度末日期
        if start.month <= 3:
            next_quarter = datetime.date(start.year, 3, 31)
        elif start.month <= 6:
            next_quarter = datetime.date(start.year, 6, 30)
        elif start.month <= 9:
            next_quarter = datetime.date(start.year, 9, 30)
        else:
            next_quarter = datetime.date(start.year, 12, 31)

        while next_quarter <= end:

            result.append(next_quarter.isoformat())
            # 找到下一个季度末日期
            if next_quarter.month == 3:
                next_quarter = datetime.date(next_quarter.year, 6, 30)
            elif next_quarter.month == 6:
                next_quarter = datetime.date(next_quarter.year, 9, 30)
            elif next_quarter.month == 9:
                next_quarter = datetime.date(next_quarter.year, 12, 31)
            else:
                next_quarter = datetime.date(next_quarter.year + 1, 3, 31)

    elif method == 'year':
        if start.month <= 6:
            next_quarter = datetime.date(start.year, 6, 30)
        else:
            next_quarter = datetime.date(start.year, 12, 31)

        # 从下一个季度末日期开始迭代，直到结束日期
        while next_quarter <= end:
            result.append(next_quarter.isoformat())

            # 找到下一个季度末日期
            if next_quarter.month == 6:
                next_quarter = datetime.date(next_quarter.year, 12, 31)
            else:
                next_quarter = datetime.date(next_quarter.year + 1, 6, 30)

    return result


# 分块多进程计算
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def new_qstart(n, day):
    """
    :param n: n>0,代表后推n个交易日，n<0，代表前推n个交易日
    :param day: 输入的日期，格式为'2010-01-01'
    :return: 新版的日期前推和后推函数
    """
    sh_exchange = mcal.get_calendar('SSE')
    if n > 0:
        day = list(x.strftime('%Y-%m-%d') for x in sh_exchange.valid_days(start_date='1900-01-01', end_date=day))[-1]
        new_day = pd.DataFrame({'date':list(x.strftime('%Y-%m-%d') for x in sh_exchange.valid_days(start_date=day, end_date='2030-01-01'))}).shift(-n).iloc[0, 0]
    else:
        new_day = pd.DataFrame({'date':list(x.strftime('%Y-%m-%d') for x in sh_exchange.valid_days(start_date='1900-01-01', end_date=day))}).shift(abs(n)).iloc[-1, 0]
    return new_day

def get_interval(date, intervals):
    date = datetime.datetime.strptime(date, '%Y-%m-%d')
    for i in range(len(intervals)-1):
        start_date = datetime.datetime.strptime(intervals[i], '%Y-%m-%d')
        end_date = datetime.datetime.strptime(intervals[i+1], '%Y-%m-%d')
        if start_date <= date < end_date:
            return intervals[i], intervals[i+1]


def extract_start_date(history, current_manager):
    records = history.split('\r\n')
    for record in records[::-1]:
        if current_manager in record:
            date_parts = record.split('(')[-1].split(')')[0].split('-')
            # 去除"至今"字样并返回开始日期
            raw_date = date_parts[0].replace('至今', '').strip()
            # 将日期转换为 YYYY-MM-DD 格式
            formatted_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
            return formatted_date
    return None


def add_new_data(index_pctChg, df_NAV_adj, fund_codes, index_code):

    previous_index_codes = index_pctChg.columns
    previous_fund_codes = df_NAV_adj.columns

    if index_code not in previous_index_codes:
        index_pctChg[index_code] = w.wsd(index_code, "pct_chg", index_pctChg.index[0], index_pctChg.index[-1], usedf=True)[1].values
    else:
        print('无需添加新的指数')

    if any(x not in previous_fund_codes for x in fund_codes):
        fundCodes = list(set(fund_codes) - ((fund_codes) & set(previous_fund_codes)))
        df_NAV_adj[fund_codes] = w.wsd(','.join(fundCodes), "NAV_adj_return1", df_NAV_adj.index[0], df_NAV_adj.index[-1], usedf=True)[1].values
    else:
        print('无需添加新基金收益')
    return index_pctChg, df_NAV_adj


def compare_(a, b):
    if a > b:
        return a
    else:
        return b


def fund_processing_logic(args):
    # Unpacking arguments
    code, df_fundTheme_secondChoose, index_pctChg_period, df_third_select_outside, self = args

    select_date = df_fundTheme_secondChoose.loc[code, '比较日期']
    if select_date >= self.initial_startDay:
        index_pctChg_period_code = index_pctChg_period.loc[select_date: self.initial_endDay].copy()
        df_third_select = df_fundTheme_secondChoose.query(f"比较日期 <= '{select_date}'").sort_values(by='任职开始日期',
                                                                                                  ascending=False)
        df_third_select.insert(0, '数据区间',
                               f"{''.join(select_date.split('-'))}-{''.join(self.initial_endDay.split('-'))}")
        fundcodes = list(df_third_select.index)
        df_NAV_adj_period = self.df_NAV_adj.loc[select_date: self.initial_endDay, fundcodes]
    else:
        df_third_select = df_third_select_outside
        fundcodes = list(df_third_select.index)
        df_NAV_adj_period = self.df_NAV_adj[fundcodes]
        index_pctChg_period_code = index_pctChg_period
    theme_pctChg = pd.concat([index_pctChg_period_code, df_NAV_adj_period], axis=1)
    # Optimized data queries
    up_df = theme_pctChg[theme_pctChg['市场环境'] == '上涨']
    middle_df = theme_pctChg[theme_pctChg['市场环境'] == '震荡']
    down_df = theme_pctChg[theme_pctChg['市场环境'] == '下跌']

    up_pct = up_df.iloc[:, 2:].applymap(lambda x: x + 1).product().rank(pct=True).loc[code] if len(
        up_df) > 0 else pd.DataFrame(index=[code], columns=['上涨'])
    middle_pct = middle_df.iloc[:, 2:].applymap(lambda x: x + 1).product().rank(pct=True).loc[code] if len(
        middle_df) > 0 else pd.DataFrame(index=[code], columns=['震荡'])
    down_pct = down_df.iloc[:, 2:].applymap(lambda x: x + 1).product().rank(pct=True).loc[code] if len(
        down_df) > 0 else pd.DataFrame(index=[code], columns=['下跌'])
    StyleBox_fund_day_theme = pd.DataFrame(data=df_third_select.loc[code]).T
    StyleBox_fund_day_theme['上涨'] = up_pct
    StyleBox_fund_day_theme['震荡'] = middle_pct
    StyleBox_fund_day_theme['下跌'] = down_pct
    return StyleBox_fund_day_theme