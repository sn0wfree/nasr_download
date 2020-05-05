# coding=utf-8
from nasr_download.nasr import Getfillings, DownloadTxt


class Run(object):
    @staticmethod
    def get_fund_info(db='NASR', table='nasr_fund_filing_info'):
        GF = Getfillings()
        GF.run(db=db, table=table)

    @staticmethod
    def get_txt(db_table='NASR.txt_getting_status.csv', count_num=100):
        DT = DownloadTxt()
        count = 0
        while 1:
            tasks = DT.get_one_task(count_num=count_num)
            print(tasks)
            DT.run_tasks(tasks=tasks, db_table=db_table, count_num=count_num)
            if count > 10:
                break
            if len(tasks) == 0:
                count += 1
            else:
                count = 0


if __name__ == '__main__':
    Run.get_txt()
    # GF = Getfillings()
    # GF.run(db='NASR', table='nasr_fund_filing_info')
    pass
