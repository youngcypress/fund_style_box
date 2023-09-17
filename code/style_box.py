# -*- coding: utf-8 -*-
"""
@Time    : 25/8/2023 下午15:00
@Author  : SongBai Li
@FileName: style_box4.py
@Software: PyCharm
@version: level4
@brief:
= 4.0.0 =
自动获取任意区间内某一主题的所有基金风格分位,每只基金的数据区间自动判断
程序更新：新增自行输入指数代码，调整程序窗口大小，提高运行速度
"""


import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
from WindPy import w
import datetime
from pathlib import Path
from function import *
from tqdm import tqdm
import tkinter as tk
from tkinter import ttk, messagebox, filedialog


#显示所有列
pd.set_option('display.max_columns',None)
#显示所有行
pd.set_option('display.max_rows',None)
#设置value的显示长度
pd.set_option('max_colwidth', 100)
#设置1000列时才换行
pd.set_option('display.width', 1000)


# 初始化Windpy
if not w.isconnected(): # 判断WindPy是否已经登录成功
    w.start()


class Fund_style(object):

    def __init__(self):

        self.path = Path(__file__).resolve().parent.parent
        self.today = datetime.datetime.now()
        self.theme_names = ['消费', '医药', '科技', '金融地产', '周期', '军工', '新能源', '大制造其他', '其他']
        self.initial_startDay = None  # 初始化查询开始日期
        self.initial_endDay = None  # 初始化查询结束日期
        self.initial_theme = None  # 初始化查询主题
        self.df_fundTheme, self.df_NAV_adj, self.reporting_dates = self.self_inspection()  # 程序初始化数据自检


    def update_fileReturn(self, document_date, today, df_NAV_adj, path):
        self.update_startDate = new_qstart(1, document_date)
        today_str = today.strftime('%Y-%m-%d')
        amend_today_str = new_qstart(0, today_str)
        if amend_today_str < today_str:
            self.update_endDate = amend_today_str  # 修正结束时间，避免出现非交易日的情况
        else:
            if today.hour > 23:   # 判断当前时间是否大于下午5点
                self.update_endDate = amend_today_str  # 修正结束时间，避免出现非交易日的情况
            else:
                self.update_endDate = new_qstart(-1, today_str)  # 修正结束时间，避免出现非交易日的情况
        # 不要重复运行
        if document_date < self.update_endDate:
            fundCodes = df_NAV_adj.columns.to_list()
            df_funds = w.wsd(','.join(fundCodes), "NAV_adj_return1", self.update_startDate,  self.update_endDate, usedf=True)[1]/100
            if self.update_startDate == self.update_endDate:
                df_funds = pd.DataFrame(data=df_funds.T.values, index=[self.update_endDate])
            else:
                pass
            df_funds.to_csv(f"{path}/data/主题基金净值收益率.csv", mode='a', header=False)
            df_funds.index.name = 'date'
            df_funds.columns = fundCodes
            df_NAV_adj = pd.concat([df_NAV_adj, df_funds])
            print('基金净值收益率更新成功')

        else:
            print('无需更新指数和基金收益数据')
        return df_NAV_adj


    def read_data(self):
        """
        预加载本地数据文件
        """
        df_fundTheme = pd.read_csv(f'{self.path}\data\各报告期主题基金分类.csv',
                                   parse_dates=['from_', 'to_', '基金成立日'])  # 该文件各个报告期的基金主题
        df_fundTheme[['from_', 'to_', '基金成立日']] = df_fundTheme[['from_', 'to_', '基金成立日']].applymap(
            lambda x: x.strftime("%Y-%m-%d"))
        df_NAV_adj = pd.read_csv(f'{self.path}\data\主题基金净值收益率.csv', index_col=['date'])  # 该文件是所有基金净值数据
        # 将索引列转换为日期时间索引，并将日期时间索引格式化为新的字符串格式
        df_NAV_adj.index = pd.to_datetime(df_NAV_adj.index, format='mixed').strftime("%Y-%m-%d")

        return df_fundTheme, df_NAV_adj

    def self_inspection(self):
        """
        程序初始化数据自检，将数据更新到最新净值
        :return:
        """
        df_fundTheme, df_NAV_adj = self.read_data()
        # 获取本地文件的最新时间
        document_date = df_NAV_adj.index[-1]
        # 获取已有的主题基金分类中的所有报告期间隔区间，需要计算的日期数据必须在这些区间内，否则没有相应的主题基金数据，无法计算
        reporting_dates = list(df_fundTheme['from_'].sort_values().unique()) + [list(df_fundTheme['to_'].sort_values().unique())[-1]]
        df_NAV_adj = self.update_fileReturn(document_date, self.today, df_NAV_adj, self.path)

        return df_fundTheme, df_NAV_adj, reporting_dates

    def first_select_themeFunds(self):

        # 获取查询日期前推三个月的交易日
        self.forward_n_Month = (pd.to_datetime(self.initial_endDay) + pd.DateOffset(months=-3)).strftime('%Y-%m-%d')
        self.from_ = get_interval(self.initial_endDay, self.reporting_dates)[0]
        df_fundTheme_fisrtChoose = self.df_fundTheme.query(
            f"from_ in {[self.from_]} & 主题 in {[self.initial_theme]} & 基金成立日 < '{self.forward_n_Month}'")
        df_endDate = w.wss(",".join(df_fundTheme_fisrtChoose['基金代码'].to_list()),
                           "fund_maturitydate_2",
                           usedf=True)[1].reset_index().rename(columns={'index': '基金代码', 'FUND_MATURITYDATE_2': '基金到期日'})
        df_fundTheme_fisrtChoose = pd.merge(df_fundTheme_fisrtChoose, df_endDate, on='基金代码')
        df_fundTheme_fisrtChoose = df_fundTheme_fisrtChoose[df_fundTheme_fisrtChoose['基金到期日'].isnull()]
        # 获取筛选的符合条件的基金中成立时间最晚的日期
        df_fundTheme_fisrtChoose = df_fundTheme_fisrtChoose.set_index('基金代码').sort_values(by='基金成立日', ascending=False)

        return df_fundTheme_fisrtChoose


    def second_select_themeFunds(self):
        df = self.first_select_themeFunds()[['基金成立日', '基金名称']]
        fundcodes = list(df.index)
        print(''.join(self.initial_endDay.split('-')))
        df_managerInfo = w.wss(",".join(fundcodes),
                    "fund_predfundmanager,fund_longestfundmanager_hist",
                    f"tradeDate={''.join(self.initial_endDay.split('-'))};order=1",
                    usedf=True)[1].rename(
            columns={'FUND_PREDFUNDMANAGER': '历任基金经理', 'FUND_LONGESTFUNDMANAGER_HIST': '现任基金经理'})
        df = pd.concat([df, df_managerInfo], axis=1)
        df['任职开始日期'] = df.apply(lambda row: extract_start_date(row['历任基金经理'], row['现任基金经理']), axis=1)
        df_fundTheme_secondChoose = df.sort_values(by='任职开始日期', ascending=False)
        return df_fundTheme_secondChoose


    def cal_fund_style(self, index_pctChg, index_code, update_progress_callback=None):

        print(f"主题:{self.initial_theme}", f"开始日期:{self.initial_startDay}", f"结束日期：{self.initial_endDay}")
        df_fundTheme_secondChoose = self.second_select_themeFunds()[['基金名称', '基金成立日', '现任基金经理', '任职开始日期']]

        print(f"指数代码:{index_code}", f"结束日期前推三个月日期:{self.forward_n_Month}", f"当前季度开始日期：{self.from_}")
        index_pctChg_period = index_pctChg.loc[self.initial_startDay:].copy()
        index_pctChg_period.loc[:, '市场环境'] = pd.cut(
            index_pctChg_period[index_code],
            bins=[index_pctChg[index_code].min(),
                  index_pctChg[index_code].quantile(1/3),
                  index_pctChg[index_code].quantile(2/3),
                  index_pctChg[index_code].max()],
            labels=['下跌', '震荡', '上涨'])
        df_fundTheme_secondChoose['基金成立日'] = pd.to_datetime(df_fundTheme_secondChoose['基金成立日'])
        df_fundTheme_secondChoose['修正成立日'] = df_fundTheme_secondChoose['基金成立日'] + pd.DateOffset(months=3)
        df_fundTheme_secondChoose[['修正成立日', '基金成立日']] = df_fundTheme_secondChoose[['修正成立日', '基金成立日']].applymap(lambda x: x.strftime('%Y-%m-%d'))
        df_fundTheme_secondChoose['比较日期'] = df_fundTheme_secondChoose.apply(lambda x: compare_(x['任职开始日期'], x['修正成立日']), axis=1)
        # Moved out of loop
        df_third_select_outside = df_fundTheme_secondChoose.query(f"比较日期 <= '{self.initial_startDay}'").sort_values(
            by='任职开始日期', ascending=False)
        df_third_select_outside.insert(0, '数据区间', f"{''.join(self.initial_startDay.split('-'))}-{''.join(self.initial_endDay.split('-'))}")

        fund_codes = list(df_fundTheme_secondChoose.index)
        StyleBox = pd.DataFrame()
        total_funds = len(fund_codes)
        for index, code in tqdm(enumerate(fund_codes)):
            if update_progress_callback:
                update_progress_callback((index + 1) / total_funds * 100)
            select_date = df_fundTheme_secondChoose.loc[code, '比较日期']
            if select_date >= self.initial_startDay:
                index_pctChg_period_code = index_pctChg_period.loc[select_date: self.initial_endDay].copy()
                df_third_select = df_fundTheme_secondChoose.query(f"比较日期 <= '{select_date}'").sort_values(by='任职开始日期', ascending=False)
                df_third_select.insert(0, '数据区间', f"{''.join(select_date.split('-'))}-{''.join(self.initial_endDay.split('-'))}")
                fundcodes = list(df_third_select.index)
                print(select_date)
                df_NAV_adj_period = self.df_NAV_adj.loc[select_date: self.initial_endDay, fundcodes]
            else:
                df_third_select = df_third_select_outside
                fundcodes = list(df_third_select.index)
                df_NAV_adj_period = self.df_NAV_adj[fundcodes]
                index_pctChg_period_code = index_pctChg_period
            theme_pctChg = pd.concat([index_pctChg_period_code, df_NAV_adj_period], axis=1)
            up_df = theme_pctChg[theme_pctChg['市场环境'] == '上涨']
            middle_df = theme_pctChg[theme_pctChg['市场环境'] == '震荡']
            down_df = theme_pctChg[theme_pctChg['市场环境'] == '下跌']
            up_pct = \
                pd.Series(np.prod(up_df.iloc[:, 2:].fillna(0).values + 1, axis=0),
                          index=up_df.columns[2:]).rank(pct=True).loc[code] if len(up_df) > 0 else pd.DataFrame(index=[code], columns=['上涨'])
            middle_pct = \
                pd.Series(np.prod(middle_df.iloc[:, 2:].fillna(0).values + 1, axis=0),
                          index=middle_df.columns[2:]).rank(pct=True).loc[code] if len(middle_df) > 0 else pd.DataFrame(index=[code], columns=['震荡'])
            down_pct = \
                pd.Series(np.prod(down_df.iloc[:, 2:].fillna(0).values + 1, axis=0),
                          index=down_df.columns[2:]).rank(pct=True).loc[code] if len(down_df) > 0 else pd.DataFrame(index=[code], columns=['下跌'])
            StyleBox_fund_day_theme = pd.DataFrame(data=df_third_select.loc[code]).T
            StyleBox_fund_day_theme['上涨'] = up_pct
            StyleBox_fund_day_theme['震荡'] = middle_pct
            StyleBox_fund_day_theme['下跌'] = down_pct
            StyleBox = pd.concat([StyleBox, StyleBox_fund_day_theme])
        StyleBox[['上涨', '震荡', '下跌']] = StyleBox[['上涨', '震荡', '下跌']].round(2)
        return StyleBox

    def update_progress(self, progress_percent):
        self.progress["value"] = progress_percent
        self.update_idletasks()  # Force redraw of the progress bar

class StyleBoxApp(tk.Tk):
    def __init__(self, fund_style_obj):
        super().__init__()

        self.fund_style = fund_style_obj
        self.funds_df = None

        self.title("Style Box Application")
        self.geometry("1000x600")

        # 初始化创建和放置小部件
        self.create_widgets()
        self.log_message('程序版本：4.0，新增自行输入指数代码的功能，修改程序窗口布局和大小，优化程序运行速度，由于“其他”类型的基金数量较多，在处理时可能存在卡顿，请在点击“生成”后不要在电脑进行其他操作，耐心等待计算完成。\n')
        self.log_message('程序启动就会进行数据自检，如果运行时间位于工作日晚上11点以后就会更新当天数据，否则只更新到前一天，如果运行时间位于周末或节假日，会更新到最新交易日\n')
        self.log_message(f"数据已更新到{self.fund_style.update_endDate}，请保证输入的结束日期不超过这个日期，嘿嘿😀\n")


    def create_widgets(self):
        # 为左侧小部件创建框架
        self.left_frame = tk.Frame(self)
        self.left_frame.grid(row=0, column=0, pady=5, padx=5)

        # 在程序右边创建日志记录的框架
        self.right_frame = tk.Frame(self)
        self.right_frame.grid(row=0, column=4, pady=5, padx=5)

        # 日期和主题的输入字段
        self.lbl_start_date = tk.Label(self.left_frame, text="输入开始日期(YYYY-MM-DD):")
        self.lbl_start_date.grid(row=0, column=0, pady=5, padx=5)
        self.entry_start_date = tk.Entry(self.left_frame)
        self.entry_start_date.grid(row=0, column=1, pady=5, padx=5)

        self.lbl_end_date = tk.Label(self.left_frame, text="输入结束日期(YYYY-MM-DD):")
        self.lbl_end_date.grid(row=1, column=0, pady=5, padx=5)
        self.entry_end_date = tk.Entry(self.left_frame)
        self.entry_end_date.grid(row=1, column=1, pady=5, padx=5)

        # Replace the theme entry with a dropdown (Combobox)
        self.lbl_theme = tk.Label(self.left_frame, text="选择主题:")
        self.lbl_theme.grid(row=0, column=2, pady=5, padx=5)

        # Get the themes from the index_dict
        themes = list(self.fund_style.theme_names)

        self.combo_theme = ttk.Combobox(self.left_frame, values=themes)
        self.combo_theme.grid(row=0, column=3, pady=5, padx=5)

        # New Entry for custom index code
        self.lbl_custom_index = tk.Label(self.left_frame, text="输入指数代码:")
        self.lbl_custom_index.grid(row=1, column=2, pady=5, padx=5)
        self.entry_custom_index = tk.Entry(self.left_frame)
        self.entry_custom_index.grid(row=1, column=3, pady=5, padx=5)

        # 创建用于运行cal_fund_style()的函数
        self.btn_cal_style = tk.Button(self.left_frame, text="生成", command=self.calculate_style)
        self.btn_cal_style.grid(row=2, column=0, pady=5, padx=5)

        # Add the progress bar below the "生成" button
        self.progress = ttk.Progressbar(self.left_frame, orient=tk.HORIZONTAL, length=300, mode='determinate')
        self.progress.grid(row=2, column=1, pady=5, padx=5)

        # Label to display the percentage
        self.progress_label = tk.Label(self.left_frame, text="0%")
        self.progress_label.grid(row=2, column=2, pady=5, padx=5)

        # 添加保存至excel的控件
        self.btn_save_excel = tk.Button(self.left_frame, text="保存至excel", command=self.save_to_excel)
        self.btn_save_excel.grid(row=2, column=3, pady=5, padx=5)

        # 创建在下方显示数据的表格
        style = ttk.Style()
        style.configure("Treeview", rowheight=42)
        self.tree = ttk.Treeview(self.left_frame, columns=("FundCode", "DatePeriod", "FundManger", "Up", "Middle", "Down"), show="headings")

        self.tree.heading("FundCode", text="基金代码")
        self.tree.heading("DatePeriod", text="数据区间")
        self.tree.heading("FundManger", text="基金经理")
        self.tree.heading("Up", text="上涨")
        self.tree.heading("Middle", text="震荡")
        self.tree.heading("Down", text="下跌")
        self.tree.column("FundCode", width=4)
        self.tree.column("DatePeriod", width=4)
        self.tree.column("FundManger", width=4)
        self.tree.column("Up", width=4)
        self.tree.column("Middle", width=4)
        self.tree.column("Down", width=4)
        # 创建在下方显示数据的表格
        self.tree.grid(row=3, column=0, columnspan=5, pady=2, padx=2, sticky="nsew")  # Adjust the row number and add columnspan

        # Treeview的滚动条,用于下拉表格
        self.scrollbar = ttk.Scrollbar(self.left_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=self.scrollbar.set)
        self.scrollbar.grid(row=3, column=5, sticky="ns")  # Use sticky option to make the scrollbar fill the vertical space

        # 用于记录的文本框
        self.log_text = tk.Text(self.right_frame, height=42, width=25)
        self.log_text.grid(row=0, column=0, pady=10, padx=10)

    def log_message(self, message):
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.see(tk.END)  # 滚动到底部

    def update_progress(self, progress_percent):
        self.progress["value"] = progress_percent
        self.progress_label.config(text=f"{progress_percent:.2f}%")  # Update the label with the current percentage
        self.update_idletasks()  # Force redraw of the progress bar and label

    def calculate_style(self):
        start_date = self.entry_start_date.get()
        end_date = self.entry_end_date.get()
        # 如果不输入日期和主题就打印错误日志
        if not start_date or not end_date:
            messagebox.showerror("请输入开始日期、结束日期！")
            return
        self.fund_style.initial_startDay = start_date
        self.fund_style.initial_endDay = end_date

        selected_theme = self.combo_theme.get()
        custom_index = self.entry_custom_index.get()
        self.fund_style.initial_theme = selected_theme

        # 判断是否输入主题指数
        # print(custom_index)
        if custom_index:
            try:
                index_pctChg = w.wsd(custom_index, "pct_chg", '2004-12-31', end_date, usedf=True)[1].rename(columns={'PCT_CHG': custom_index})/100
                if index_pctChg.isnull().all().iloc[0]:
                    messagebox.showerror("请输入正确的Wind代码！")
                else:
                    index_pctChg.index.name = custom_index
                    index_pctChg = index_pctChg[index_pctChg[custom_index].notnull()].iloc[1:]
                    index_pctChg.index = pd.to_datetime(index_pctChg.index).strftime("%Y-%m-%d")
                    self.style_df = self.fund_style.cal_fund_style(index_pctChg, custom_index, self.update_progress)
                    # 清除以前的数据并将新数据插入到树中
                    for row in self.tree.get_children():
                        self.tree.delete(row)

                    for index, row in self.style_df.iterrows():
                        self.tree.insert("", tk.END,
                                         values=(index, row['数据区间'], row['现任基金经理'], row['上涨'], row['震荡'], row['下跌']))
                    # 打印日志
                    self.funds_df = self.fund_style.second_select_themeFunds()
                    funds_list = list(self.funds_df.index)
                    self.log_message(f"主题：{selected_theme}，指数：{custom_index}，开始日期：{start_date}, 结束日期：{end_date}\n")
                    self.log_message(f"在{start_date}-{end_date}区间和{selected_theme}主题下，找到{len(funds_list)}基金\n")
                    self.log_message(f"如要计算其他类型的主题，请重新输入主题即可\n")
            except:
                messagebox.showerror("你的wind api已限额！")
        else:
            messagebox.showerror("请选择主题指数！")
    def save_to_excel(self):
        if self.style_df is None:
            messagebox.showerror("Error", "No data to save!")
            return

        filename = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                filetypes=[("Excel files", "*.xlsx"), ("All Files", "*.*")])
        if filename:
            self.style_df.to_excel(filename)
            # 打印日志
            self.log_message(f"Saved results to {filename}")

fund_style_obj = Fund_style()
app = StyleBoxApp(fund_style_obj)
app.mainloop()

