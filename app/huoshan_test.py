import hashlib
import requests
import time


import hashlib
import time


class HuoshanHTTPDNSResolver:
    def __init__(self, account_id, secret):
        self.base_url = "https://httpdns.volcengineapi.com/resolve"
        self.account_id = account_id
        self.secret = secret

    def generate_sign(self, dn, timeStamp, cip='', t=''):
        """
        Generates the md5 signature required for the HTTP DNS request.
        """
        old = (self.secret, timeStamp, self.account_id, dn, cip, t)
        new = sorted(old)
        seperator = "_"
        newStr = seperator.join(new)
        hl = hashlib.md5()
        hl.update(newStr.encode(encoding='utf-8'))
        return hl.hexdigest()

    def resolve_domain(self, dn, ip='', t=''):
        """
        Resolves the provided domain using HTTP DNS service.

        :param session: The aiohttp session.
        :param dn: The domain name to resolve.
        :param ip: The client's IP address (optional). If not provided, the server will use the client's real public IP.
        :param t: Optional parameter to specify type.
        :return: A JSON object with the resolution result.
        """
        timeStamp = str(int(1000 * (time.time() + 3600)))
        sign = self.generate_sign(dn, timeStamp, ip, t)

        params = {
            "domain": dn,
            "account_id": self.account_id,
            "sign": sign,
            "timestamp": timeStamp
        }
        if ip:
            params["ip"] = ip
        if t:
            params["type"] = t

        response = requests.get(self.base_url, params=params)
        return response.json()


if __name__ == '__main__':
    # HuoshanYun
    resolver = HuoshanHTTPDNSResolver(account_id="2102411820", secret="rU195Gb2aWaHdnfs")
    # To resolve a domain
    response = resolver.resolve_domain(dn="google.com")
    print(response)
