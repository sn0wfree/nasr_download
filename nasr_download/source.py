# coding=utf-8
from nasr_download.utils.MySQLConn_v004_node import MySQLNode

mysql_settings = dict(host='106.13.205.210', port=3306, user='linlu', passwd='Imsn0wfree', db='NASR')


class Source(object):
    NASR = MySQLNode('MySQLNode', **mysql_settings)


if __name__ == '__main__':
    pass
