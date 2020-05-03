# coding=utf-8
def chunk(df, n):
    if hasattr(df, '__len__'):
        pass
    else:
        raise ValueError('must have __len__ attribute')
    length = len(df)
    for i in range(0, length, n):
        yield df[i:i + n]


if __name__ == '__main__':
    pass
