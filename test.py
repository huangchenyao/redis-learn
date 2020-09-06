import functools


def log1(fun):
    @functools.wraps(fun)
    def wrapper(*args, **kwargs):
        print('log ' + fun.__name__)
        return fun(*args, **kwargs)

    return wrapper


def log2(text):
    def decorator(fun):
        @functools.wraps(fun)
        def wrapper(*args, **kwargs):
            print('log ' + text)
            return fun(*args, **kwargs)

        return wrapper

    return decorator


# @log1
# @log2('123')
def hello():
    print('hello world')


if __name__ == '__main__':
    h = log2('111')(hello)
    print(h.__name__)
