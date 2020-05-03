# coding=utf-8
import time
from functools import wraps


def conn_try_again(max_retires=5, default_retry_delay=1,
                   default_sleep_time=0.1, Exception_func=Exception):
    def _conn_try_again(func):
        RETIES = 0
        count = {'num': RETIES}  # retry couter

        @wraps(func)
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception_func as e:
                c = count['num']
                if c < max_retires:
                    sleep_time = default_sleep_time + count['num'] * default_retry_delay
                    time.sleep(sleep_time)
                    count['num'] += 1

                    print(
                        f'Found target Error! func will wait {sleep_time} seconds and retry! current retries count: {c}')
                    return wrapped(*args, **kwargs)
                else:
                    raise e
            return wrapped

        return _conn_try_again


if __name__ == '__main__':
    pass
