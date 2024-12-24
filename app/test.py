import hashlib
import requests
import time


class BaiduHTTPDNSResolver:
    def __init__(self, account_id, secret):
        self.base_url = "http://180.76.76.200/v3/resolve"
        self.account_id = account_id
        self.secret = secret

    def generate_sign(self, dn, t):
        """
        Generates the md5 signature required for the HTTP DNS request.
        """
        sign_str = f"{dn}-{self.secret}-{t}"
        return hashlib.md5(sign_str.encode()).hexdigest()

    def resolve_domain(self, dn, ip=None):
        """
        Resolves the provided domain using HTTP DNS service.

        :param dn: The domain name to resolve.
        :param ip: The client's IP address (optional). If not provided, the server will use the client's real public IP.
        :return: A JSON object with the resolution result.
        """
        t = str(int(time.time()) + 300)  # Current timestamp + 5 minutes
        sign = self.generate_sign(dn, t)

        params = {
            "account_id": self.account_id,
            "dn": dn,
            "sign": sign,
            "t": t
        }

        if ip:
            params['ip'] = ip

        response = requests.get(self.base_url, params=params)
        return response.json()


# BaiduYun
resolver = BaiduHTTPDNSResolver(account_id="137279", secret="sbDYgAEM7JXVv7xpgo18")
# To resolve a domain
response = resolver.resolve_domain(dn="baidu.com")
print(response)