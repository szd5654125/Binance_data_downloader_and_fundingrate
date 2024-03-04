import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def calc_funding(um_or_cm):
    # 用于存储所有结果的DataFrame
    all_data = pd.DataFrame(columns=['day_num', 'funding_rate_sum', 'daily_earn', '1m_daily_earn', '3m_daily_earn', 'signal'])

    # 定义um_data文件夹的路径
    parent_folder_path = os.path.join(os.getcwd(), f'{um_or_cm}_data')

    # 获取当前位置所有符合条件的文件夹
    folders = [f for f in os.listdir(parent_folder_path) if f.startswith(f'futures_{um_or_cm}_monthly_fundingRate') and not f.endswith('_zip')]

    # 遍历每个文件夹
    for folder in folders:
        path = os.path.join(parent_folder_path, folder)
        all_rates = []
        rates_1m = []
        rates_3m = []

        # 获取文件夹名作为signal
        signal = folder.replace('futures_', '').replace('monthly_fundingRate_', '')

        # 初始化calc_time
        first_calc_time = None
        last_calc_time = None

        # 获取文件夹内所有CSV文件
        csv_files = [file for file in os.listdir(path) if file.endswith('.csv')]

        # 遍历文件夹中的CSV文件
        if csv_files:
            # 读取第一个文件的第一个calc_time作为起始时间
            first_df = pd.read_csv(os.path.join(path, csv_files[0]))
            first_calc_time = pd.to_datetime(first_df.iloc[0]['calc_time'], unit='ms')

            # 读取最后一个文件的最后一个calc_time作为结束时间
            last_df = pd.read_csv(os.path.join(path, csv_files[-1]))
            last_calc_time = pd.to_datetime(last_df.iloc[-1]['calc_time'], unit='ms')

            # 计算一月前和三月前的时间点
            one_month_ago = last_calc_time - timedelta(days=30)
            three_months_ago = last_calc_time - timedelta(days=90)
            # 遍历文件夹中的CSV文件
            for file in csv_files:
                file_path = os.path.join(path, file)
                df = pd.read_csv(file_path)
                df['calc_time'] = pd.to_datetime(df['calc_time'], unit='ms')

                # 计算总funding_rate_sum
                all_rates.extend(df['last_funding_rate'].tolist())

                # 筛选近一个月和近三个月的记录
                rates_1m.extend(df[df['calc_time'] >= one_month_ago]['last_funding_rate'].tolist())
                rates_3m.extend(df[df['calc_time'] >= three_months_ago]['last_funding_rate'].tolist())

        # 计算经历的天数
        if first_calc_time is not None and last_calc_time is not None:
            day_num = (last_calc_time - first_calc_time).total_seconds() / (24 * 3600)  # 假设calc_time是以毫秒为单位的时间戳
        else:
            day_num = 0
        funding_rate_sum = np.sum(all_rates)
        daily_earn = funding_rate_sum / day_num

        # 计算近一个月和近三个月的日均资金费率
        daily_earn_1m = np.sum(rates_1m) / 30
        daily_earn_3m = np.sum(rates_3m) / 90

        # 添加新行到DataFrame
        new_row = pd.DataFrame({
            'day_num': [day_num],
            'funding_rate_sum': [funding_rate_sum],
            'daily_earn': [daily_earn],
            '1m_daily_earn': [daily_earn_1m],
            '3m_daily_earn': [daily_earn_3m],
            'signal': [signal]
        })

        # 使用concat而不是append
        all_data = pd.concat([all_data, new_row], ignore_index=True)

    # 输出为CSV文件
    output_filename = f'{datetime.now().strftime("%Y%m%d%H%M%S")}_funding_rate.csv'
    # 输出文件路径包括um_data文件夹
    output_file_path = os.path.join(parent_folder_path, output_filename)
    all_data.to_csv(output_file_path, index=False)

    return all_data


# 调用函数
df_result = calc_funding('um')