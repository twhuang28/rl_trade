#!/usr/bin/env python
# coding: utf-8

import os
import datetime
import pandas as pd

class Resample:
    """
        walk through all raw data, compared with dates of option DM and check file that haven't been extracted
    """
    def __init__(self, item: str, source_data_path: str):
        """
            Args:
                item: str, FUTURE, OPTION
        """
        self.item = item
        self.source_data_path = source_data_path
        self.feature_list = ['open', 'high', 'low', 'close', 'volume']

    @staticmethod
    def strip_eof(df: pd.DataFrame) -> pd.DataFrame:
        """
            check eof
            Args:
                df
        """
        lastrow = df.iloc[-1]
        if lastrow[0] == '\x1a' and lastrow[1:].isnull().all():
            return df.drop([lastrow.name], axis=0)
        return df

    def get_col_name(self, df: pd.DataFrame) -> list:
        """
            check column number and rename
        """
        if self.item == 'FUTURE':
            columns = [
                'txd_dt', 'item_code', 'contract_mon',
                'txd_tm', 'price', 'volume',
                'nearby_price', 'back_price'
            ]
        elif self.item == 'OPTION': 
            columns = [
                'txd_dt', 'item_code', 'strike_price',
                'contract_mon', 'cp_flag', 'txd_tm',
                'price', 'volume'
            ]
        else:
            raise ValueError("please input item='FUTURE' or 'OPTION'")
        if len(df.columns) == 9:
            columns.append('open_flag')
        return columns

    def resample_tick_data(self, filename: str, intraday_flag: bool=True,
                           freq: str='D', label: str='left'):
        """
            filepath: the path of .rpt or .csv
            intraday_flag: when True imply intraday trading, False imply after-hour trading
            freq: resample frequency, can be day(D), minite(T)...
            label: use left or right datetime to represent resampling data
        """
        
        # set parameter
        if self.item == 'FUTURE':
            skiprows = 1
            item_list = [
                'TX', 'MTX', 'TE', 'TF', 'XIF'
            ]
            groupby_condition = [
                'item_code', 'contract_mon'
            ]
        elif self.item == 'OPTION': 
            skiprows = 2
            item_list = [
                'TXO', 'TEO', 'TFO', 'XIO'
            ]
            groupby_condition = [
                'item_code', 'contract_mon',
                'strike_price', 'cp_flag'
            ]
        else:
            raise ValueError("please input item='FUTURE' or 'OPTION'")
        dtype = {
            'txd_dt':str,
            'txd_tm':str
        }
        print(f"start resample {filename}")
        
        # check column number and rename
        filepath = os.path.join(self.source_data_path, filename)
        df_header = pd.read_csv(
            filepath,
            compression='zip',
            encoding='big5',
            nrows=1
        )
        df = pd.read_csv(
            filepath, 
            compression='zip', 
            encoding='big5',
            skiprows=skiprows, 
            dtype=dtype, 
            names=self.get_col_name(df_header)
        )
        df = self.strip_eof(df)
        # change dtype and transform format
        df['item_code'] = df['item_code'].str.strip().astype('category')
        df['txd_dt'] = df['txd_dt'].str.strip()
        df['txd_tm'] = df['txd_tm'].str.strip()
        if self.item == 'FUTURE':
            df = df[(df['contract_mon'].str.find('/') < 0)]
        elif self.item == 'OPTION': 
            df['cp_flag'] = df['cp_flag'].str.strip().astype('category')
            if df['contract_mon'].dtype == object:
                df['contract_mon'] = df['contract_mon'].str.strip().astype('category')
        else:
            raise ValueError("please input item='FUTURE' or 'OPTION'")
        
        # set datetime index
        df['txd_tm'] = df['txd_tm'].str.slice(0,6)
        df = df.set_index(pd.to_datetime(df['txd_dt'] + df['txd_tm'], format='%Y%m%d%H%M%S'))
        df = df.drop(['txd_dt', 'txd_tm'], axis=1)
        df = df.sort_index(axis=0, ascending=True)
        df.index.name = 'date'

        # check intraday or after-hour trading and filter datetime
        if intraday_flag:
            df = df[datetime.time(8, 45, 0): datetime.time(13, 45, 0)]
        else:
            df = df[datetime.time(15, 0, 0): datetime.time(5, 0, 0)]
            df.index = df.index.shift(-6, freq='H')

        df = df[df['item_code'].isin(item_list)]
        
        if df.shape[0] > 0:    
            df_ohlc = df.groupby(groupby_condition)['price'].resample(freq, closed = 'left', label=label).ohlc()
            df_vol = df.groupby(groupby_condition)['volume'].resample(freq, closed = 'left', label=label).sum() / 2
            df_re = pd.concat([df_ohlc, df_vol], axis=1)
        else :
            df_re = pd.DataFrame([])

        df_re = df_re.reset_index()
            
        return df_re

    def filter_nearby_item(self, df, item_code):
        """
        """
        df = df.set_index('date')
        df = df[df['item_code'] == item_code]
        df_nearby = df.groupby('date')[['contract_mon']].min()
        df_nearby = df_nearby.rename(columns={'contract_mon': 'nearby_contract_mon'})
        df = df.join(df_nearby, how='left')
        df = df[df['contract_mon'] == df['nearby_contract_mon']]
        return df[self.feature_list]
    
    def run(self, item_code, intraday_flag=True, freq='D', label='left'):
        """
            resample all todo data and append to option DM
        """
        df = pd.DataFrame([])
        todo_path_list = os.listdir(self.source_data_path)
        if todo_path_list:
            for filename in todo_path_list:
                if filename.endswith('.zip'):
                    df_re = self.resample_tick_data(
                        filename,
                        intraday_flag=intraday_flag,
                        freq=freq,
                        label=label
                    )
                    if df_re.shape[0] > 0:
                        df = df.append(df_re, sort=False, ignore_index=True)
                    if self.item == 'FUTURE':
                        df = df.sort_values(['date', 'item_code', 'contract_mon'])
                    elif self.item == 'OPTION':
                        df = df.sort_values(['date', 'item_code', 'contract_mon', 'strike_price', 'cp_flag'])
                    else:
                        raise ValueError("please input item='FUTURE' or 'OPTION'")
                    df = df.reset_index(drop=True)
            df = self.filter_nearby_item(df, item_code)
            return df
        else:
            print(f"All {self.item} tick data have been resampled")

if __name__ == '__main__':
    item = 'FUTURE'
    item_code = 'TX'
    source_data_path = '/Users/tw/Documents/tx_vol/data/future'
    resample = Resample(item, source_data_path)
    df = resample.run(item_code, freq='15T')
