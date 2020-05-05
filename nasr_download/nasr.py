# coding=utf-8

import os
import sqlite3
import time
import uuid

import numpy as np
import pandas as pd
from functools import lru_cache
import data
from nasr_download.header import HeaderTools
from nasr_download.source import Source
from nasr_download.utils.chunk_tools import chunk


#
# def chunk(df, n):
#     if hasattr(df, '__len__'):
#         pass
#     else:
#         raise ValueError('must have __len__ attribute')
#     length = len(df)
#     for i in range(0, length, n):
#         yield df[i:i + n]


class CIKGet(HeaderTools):
    def __init__(self, sqlite_path='/Users/sn0wfree/PycharmProjects/nasr_download/file/edgarticker.sqlite'):
        # self.tasks = self.load_sqlite(sqlite_path)
        self._get_10_tasks = self.load_ticker_mysql()
        pass

    @staticmethod
    def load_ticker(path='/Users/sn0wfree/PycharmProjects/nasr_download/file/edgarticker.csv', dtype='csv'):
        return pd.read_csv(path)

    @staticmethod
    def load_ticker_mysql(tasks_db_table='NASR.tasks', limit=10):
        sql = f"select fund_ticker from {tasks_db_table} where self_id >= (SELECT floor(RAND() * (SELECT MAX(self_id) FROM {tasks_db_table}))) limit {limit} "
        return Source.NASR.sql2data(sql)['fund_ticker'].values.tolist()

    @classmethod
    def store_sqlite_init(cls, path='/Users/sn0wfree/PycharmProjects/nasr_download/file/edgarticker.csv',
                          store_path='/Users/sn0wfree/PycharmProjects/nasr_download/file/edgarticker.sqlite'):
        if os.path.isfile(store_path):
            pass
        else:
            df = cls.load_ticker(path)
            df['status'] = 0
            with sqlite3.connect(store_path) as conn:
                df.to_sql('fund_ticker', conn)

    @staticmethod
    def load_sqlite(store_path='/Users/sn0wfree/PycharmProjects/nasr_download/file/edgarticker.sqlite'):
        with sqlite3.connect(store_path) as conn:
            return pd.read_sql('select distinct fund_ticker from fund_ticker', conn).values.ravel().tolist()

    @staticmethod
    def get_tick_url(ticker,
                     base_url="https://www.sec.gov/cgi-bin/series?ticker={ticker}&CIK=&sc=companyseries&type=N-PX&Find=Search"):
        url = base_url.format(ticker=ticker)
        return url

    @classmethod
    def parse_urls(cls, urls):
        res = cls.get_urls(urls)
        return list(res)

    @classmethod
    def prepare(cls, ticker_list: list):

        func = cls.get_tick_url
        ticker_urls = ((ticker, func(ticker)) for ticker in ticker_list)

        return ticker_urls

    @classmethod
    def batch_get(cls, ticker_urls: list):
        tickers, urls = zip(*ticker_urls)
        r = dict(zip(urls, tickers))

        for urls, resp in CIKGet.parse_urls(urls):
            tckr = r[urls]
            yield tckr, urls, resp

    @classmethod
    def whole_parse(cls, ticker_urls):
        for ticker, urls, resp in cls.batch_get(ticker_urls):
            if resp is None:
                print(f'error: {ticker}')
                pass
            else:
                cik = cls.translate_ticker_resp_2_cik(resp)
                if cik is None:
                    yield ticker, urls, cik
                else:
                    yield ticker, urls, cik

    @classmethod
    def translate_ticker_resp_2_cik(cls, resp, xpath="/html/body/div/table/tr[@valign='top']/*/a"):
        obj = cls.parser(resp.text)
        sdr = cls.get_trs(obj, xpath)
        for sd in sdr:
            if sd.attrib['class'] == 'search' and sd.attrib.get('href'):
                cik = sd.xpath('text()')[0]
                break
            else:
                pass
        else:
            cik = None
        return cik


class Getfillings(CIKGet):
    def __init__(self, sqlite_path='/Users/sn0wfree/PycharmProjects/nasr_download/file/edgarticker.sqlite'):
        super(Getfillings, self).__init__(sqlite_path=sqlite_path)

    @staticmethod
    def get_url_from_cik(cik,
                         base_url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=NSAR-A&dateb=&count=100&scd=filings"):
        return base_url.format(cik=cik)

    @classmethod
    def parser_cik_url(cls, resp, xpath='//*[@id="seriesDiv"]/table/*/td/a'):
        obj = cls.parser(resp.text)
        sdr = cls.get_trs(obj, xpath)

        df_holder = None
        for df in pd.read_html(resp.text):
            if 'Filings' in df.columns:
                df_holder = df
                break
        if df_holder is None:
            return None, None, None
        else:
            link_df = pd.DataFrame([(items.attrib['href'], items.text.strip()) for items in sdr],
                                   columns=['link', 'dtype'])
            link_df = link_df[link_df['dtype'] == 'Documents']

            df_holder['SECAccessionNo'] = df_holder['Description'].apply(
                lambda x: x.split("companiesAcc-no: ")[-1].strip()[0:20])
            link_df['SECAccessionNo'] = link_df['link'].apply(lambda x: x.split("/")[-1][0:20])
            df2 = pd.merge(link_df, df_holder, on=['SECAccessionNo'])
            return df2, link_df, df_holder

    @classmethod
    def get_final_urls(cls, ticker_list):
        # for ticker_url in cls.prepare(ticker_list):
        print('prepare ticker! get ticker urls')
        ticker_urls = list(cls.prepare(ticker_list))
        print('got ticker urls')
        # tasks = [(ticker, ticker_url, cik, cls.get_url_from_cik(cik)) for ticker, ticker_url, cik in
        #          ]
        for ticker, ticker_url, cik in cls.whole_parse(ticker_urls):
            if cik is None:
                print(f'ticker:{ticker} got nothing!')
                sql = f"INSERT INTO `error_ticker` (`ticker`) VALUES ('{ticker}')"
                Source.NASR.Excutesql(sql)

            else:
                print(f'got cik:{cik}')
                cik_url_list = cls.get_url_from_cik(cik)
                if isinstance(cik_url_list, list):
                    print(f'got cik_url:{cik_url_list[0]}')
                else:
                    print(f'got cik_url:{cik_url_list}')
                for u, resp in cls.parse_urls(cik_url_list):
                    df2, link_df, df_holder = cls.parser_cik_url(resp)
                    print(df2)
                    if not df2.empty:
                        print(f'got cik:{cik} info')
                        df2['ticker'] = ticker
                        df2['ticker_url'] = ticker_url
                        df2['cik'] = cik
                        df2['cik_url'] = u
                        yield df2.rename(columns={'Filing Date': 'Filing_Date', 'File/Film Number': 'File_Film_Number'})

    @staticmethod
    def store_final_url(df2, db, table):
        Source.NASR.df2sql(df2, table, db=db, csv_store_path='./')

    @classmethod
    def _main_process(cls, ticker_list, db='NASR', table='nasr_fund_filing_info'):
        try:
            res = list(Getfillings.get_final_urls(ticker_list))
            c = pd.concat(res)
            cls.store_final_url(c, db, table)
        except Exception as e:
            print(str(e))
            pass

    # @conn_try_again(max_retires=5, default_retry_delay=1, default_sleep_time=0.1, Exception_func=ValueError)
    def run(self, db='NASR', table='nasr_fund_filing_info'):
        while 1:
            tasks_list = self.load_ticker_mysql()
            print(tasks_list)
            if len(tasks_list) != 0:
                for tasks in chunk(tasks_list, 1):
                    print('run: {}'.format(','.join(tasks)))
                    self._main_process(tasks, db=db, table=table)
                    print('run: {} done!'.format(','.join(tasks)))
                    time.sleep(10)
            else:
                raise ValueError('tasks get empty!')


# ticker_list2, ticker_url_list, cik_list, cik_url_list = zip(*tasks)
# for i, (u, resp) in enumerate(cls.parse_urls(cik_url_list)):
#     df2, link_df, df_holder = cls.parser_cik_url(resp)
#     yield ticker_list2[i], ticker_url_list[i], cik_list[i], u, df2


# update_sql = f"UPDATE {db}.{table} SET status = 1 WHERE href = '{rest}' and status = 0 "


class DownloadTxt(HeaderTools):
    def __init__(self):

        pass

    @staticmethod
    @lru_cache(maxsize=100)
    def pool(a):
        sql = 'select distinct self_id from `txt_tasks` '
        self_id_df = Source.NASR.sql2data(sql)
        if self_id_df.empty:
            return None
        else:
            return self_id_df['self_id'].values.tolist()

    @classmethod
    def get_one_task(cls, count_num=10):
        a = pd.datetime.now().strftime("%m")
        rand_num_list = np.random.choice(cls.pool(a), size=count_num)
        rand_num_str = ','.join(map(lambda x: str(x), rand_num_list))

        sql = f"SELECT * FROM `txt_tasks` where self_id in ({rand_num_str}) "
        return Source.NASR.sql2data(sql)

    @staticmethod
    def generate_tasks(tasks_df):
        for item_info in tasks_df.to_dict('records'):
            full_link = item_info['full_link']
            cik = item_info['cik']
            File_Film_Number = item_info['File_Film_Number']
            hash_link_code = item_info['hash_link_code']
            SECAccessionNo = item_info['SECAccessionNo']
            ticker = item_info['ticker']
            respond_txt_url = full_link.split(SECAccessionNo)[0] + SECAccessionNo + '.txt'

            yield respond_txt_url, ticker, cik, SECAccessionNo, hash_link_code

    @classmethod
    def parser_tasks(cls, tasks, store_path):
        print(tasks)
        respond_txt_url_list, ticker_list, cik_list, SECAccessionNo_list, hash_link_code_list = zip(*tasks)
        finder_code = dict(zip(respond_txt_url_list, hash_link_code_list))
        finder_ticker = dict(zip(respond_txt_url_list, ticker_list))
        finder_cik = dict(zip(respond_txt_url_list, cik_list))
        finder_SECAccessionNo = dict(zip(respond_txt_url_list, SECAccessionNo_list))

        # for url_list in chunk(respond_txt_url_list, 10):
        # for respond_txt_url, ticker, cik, SECAccessionNo, hash_link_code in tasks:

        for url, resp in cls.get_urls(respond_txt_url_list):
            hashed_name = uuid.uuid5(uuid.NAMESPACE_DNS, url).hex + f'_{finder_SECAccessionNo[url]}.txt'
            yield finder_code[url], finder_ticker[url], finder_cik[url], url, hashed_name
            cls.save(store_path + '/' + hashed_name, resp.content)

    @classmethod
    def run_tasks(cls, tasks=None, store_path=None, db_table='NASR.txt_getting_status.csv', count_num=10):
        if store_path is None:
            store_path = data.__path__[0]
        print(store_path)
        if tasks is None:
            tasks = cls.get_one_task(count_num=count_num)
        tasks = cls.generate_tasks(tasks)
        for hash_link_code, ticker, cik, url, hashed_name in cls.parser_tasks(tasks, store_path):
            print(f'{url} done!')
            insert_sql = f"INSERT ignore INTO {db_table} (`ticker`, `cik`, `hash_link_code`, `store_name`)  VALUES('{ticker}', '{cik}', '{hash_link_code}', '{hashed_name}')"
            # where_clause1 = f"ticker = '{ticker}' and cik = '{cik}' and hash_link_code = '{hash_link_code}'"
            # update_sql = f"UPDATE {db_table} SET status = 1 WHERE {where_clause1} and status = 0 "
            Source.NASR.Excutesql(insert_sql)
        time.sleep(1)

    @staticmethod
    def save(name: str, content):
        with open(name, 'wb+') as f:
            f.write(content)

            # resp = DT.get_url(respond_txt)
            # content = resp.content
            #
            # print(1)


if __name__ == '__main__':
    url1 = 'https://www.sec.gov/cgi-bin/series?ticker=FCAKX&CIK=&sc=companyseries&type=N-PX&Find=Search'
    url2 = 'https://www.sec.gov/cgi-bin/series?ticker=FEIKX&CIK=&sc=companyseries&type=N-PX&Find=Search'
    # s = CIKGet.parse_urls(url1)
    url3 = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000802716&type=NSAR-A&dateb=&count=100&scd=filings'

    # for u, resp in CIKGet.parse_urls(url3):
    #     #     df2, link_df, df_holder = Getfillings.parser_cik_url(resp)
    DT = DownloadTxt()
    DT.run_tasks(count_num=2)

    # sdf = res[0]
    # full_link = sdf['full_link'][0]
    # cik = sdf['cik'][0]
    # File_Film_Number = sdf['File_Film_Number'][0]
    # SECAccessionNo = sdf['SECAccessionNo'][0]
    # ticker = sdf['ticker'][0]
    # respond_txt = full_link.split(SECAccessionNo)[0] + SECAccessionNo + '.txt'
    # resp = DT.get_url(respond_txt)
    # content = resp.content
    # with open('text', 'wb+') as f:
    #     f.write(content)

    # def load_ticker(path='/Users/sn0wfree/PycharmProjects/nasr_download/file/edgarticker.csv', dtype='csv'):
    #     return pd.read_csv(path)

    # df = load_ticker()
    # Getfillings.store_final_url(df, 'NASR', 'ticker')

    # GF.run(db='NASR', table='nasr_fund_filing_info')
    # for tasks in chunk(GF.tasks, 4):
    #     print('run: {}'.format(','.join(tasks)))
    #     Getfillings.main_process(tasks)
    #     print('run: {} done!'.format(','.join(tasks)))
    #     time.sleep(10)

    # for url, resp in s:
    #     # print(url)
    #     obj = CIKGet.parser(resp.text)
    #     sdr = CIKGet.get_trs(obj, xpath)
    #     for sd in sdr:
    #         if sd.attrib['class'] == 'search' and sd.attrib.get('href'):
    #             cik = sd.xpath('text()')[0]
    #             break
    #         else:
    #             pass
    #     else:
    #         cik = None
    #     print(url, cik)

    # {'class': 'search', 'href': '/cgi-bin/browse-edgar?CIK=0000275309&action=getcompany'}

    pass
