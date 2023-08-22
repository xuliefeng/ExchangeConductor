import os


class ProxyRotator:
    DEFAULT_FILE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'proxy_http_ip.txt')

    def __init__(self, filepath=DEFAULT_FILE_PATH):
        self.proxies = self.load_proxies_from_file(filepath)
        self.index = 0

    @staticmethod
    def load_proxies_from_file(filepath):
        with open(filepath, 'r') as file:
            return [line.strip() for line in file.readlines()]

    def get_next_proxy(self):
        if self.index >= len(self.proxies):
            self.index = 0
        proxy = self.proxies[self.index]
        self.index += 1
        return {"http://": proxy, "https://": proxy}
