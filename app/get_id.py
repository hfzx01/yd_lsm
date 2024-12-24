import socket
import fcntl
import struct
import uuid
import subprocess
import re


def get_ip():
    result = subprocess.run(['curl', 'cip.cc'], capture_output=True, text=True)
    if result.returncode == 0:
        ip_match = re.search(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', result.stdout)
        if ip_match:
            ip_address = ip_match.group()
            return ip_address
        else:
            print("ip match failed")
    else:
        print("get ip failed")


def get_ipv4_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15].encode('utf-8'))
        )[20:24])
    except IOError:
        return None


def get_mac_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        info = fcntl.ioctl(
            s.fileno(),
            0x8927,  # SIOCGIFHWADDR
            struct.pack('256s', ifname[:15].encode('utf-8'))
        )
        mac = ':'.join(['%02x' % b for b in info[18:24]])
        return mac
    except IOError:
        return None


def get_unique_identifier():
    # Try to get the addresses for common network interfaces
    ipv4 = get_ip()
    for ifname in ['eth0', 'ens3', 'wlan0', 'en0', 'wlp2s0', 'ens192']:
        mac = get_mac_address(ifname)
        if ipv4 and mac:
            return ipv4 + "-" + mac

    # Fallback to the MAC address from uuid if no network interface is found
    mac = ':'.join(['{:02x}'.format((uuid.getnode() >> elements) & 0xff)
                    for elements in range(0, 2 * 6, 2)][::-1])
    return None, mac


if __name__ == "__main__":
    print(get_ip())
