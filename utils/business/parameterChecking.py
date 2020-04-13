import unittest

from interfaces import interfaces


class ParameterChecking(unittest.TestCase):
    def __init__(self, url, is_get_method, **params):
        self.url = url
        self.params = params
        self.is_get_method = is_get_method

    def needed_params_checking(self, error_no="100"):
        headers = {}
        data = {}
        for param, value in self.params.items():
            if param == "headers":
                headers.setdefault("headers", value)
            else:
                data.setdefault(param, value)

        param = data.copy()
        for k, v in param.items():
            data.pop(k)
            r = interfaces.request(self.url, data=data, is_get_method=self.is_get_method, headers=headers)
            self.assertEqual(first=str(r.get("error_no")), second=error_no,
                             msg="必填项，error_no的预期值是%s, 当前为%s" % (str(r.get("error_no")), error_no))
            data.setdefault(k, v)

