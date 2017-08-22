from functools import wraps


def data_provider(data_provider_f):
    def dec(f):
        @wraps(f)
        def wrp(self, *args, **kwargs):
            for index, x_args in enumerate(data_provider_f(self)):
                try:
                    f(self, *x_args)
                except AssertionError as e:
                    raise e
        return wrp
    return dec
