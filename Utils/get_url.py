import requests
from fake_useragent import UserAgent


class GetUrl:
    def __init__(self):
        self.account_list = ['cjNZdnUxdG1wc0ZUbjVJOTpOS24wMlY0QnppSG5iYzc3', 'NkEzVXVhQU5QOElnQnBpNjo5ZzBhSXNHWlJYNHBldzFP']
        self.account_num = 0

    def get_response(self, url, cookie=None, referer=None):
        # 伪装请求
        ua = UserAgent()
        proxies = {"https": "http://secondtransfer.moguproxy.com:9001", "http": "http://secondtransfer.moguproxy.com:9001"}
        headers = {
            'Connection': 'keep-alive',
            'Proxy-Authorization': 'Basic %s' % self.account_list[self.account_num],
            'User-Agent': ua.random
        }
        if cookie is not None:
            headers['Cookie'] = cookie
        if referer is not None:
            headers['Referer'] = referer
        response = requests.get(url=url, headers=headers, proxies=proxies, verify=False, allow_redirects=False,
                                timeout=20)

        if response.status_code is not 200:
            return response.status_code

        return response

    def change_account(self):
        if self.account_num == 0:
            self.account_num = 1
        else:
            self.account_num = 0
