import socket
from datetime import datetime

ip_port = ("192.168.100.129", 6234)
buffer_size = 1024
udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # 数据报
udp_server.bind(ip_port)
while True:
    data, address = udp_server.recvfrom(buffer_size)
    command = ("date -s \"%s\"" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("address: ", address)
    print("data: ", data.decode("utf-8"))
    print("command: ", command)
    udp_server.sendto(command.encode("utf-8"), address)
