# coding=utf-8
from nasr_download.nasr import Getfillings

if __name__ == '__main__':
    GF = Getfillings()
    GF.run(db='NASR', table='nasr_fund_filing_info')
    pass
