from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import StreamCallback
from org.apache.commons.io import IOUtils
from struct import *
import binascii
import json
import time

hex_char = "0123456789ABCDEF"


def byte_to_hex_string(byte_array):
    hex_array = []
    for i in range(len(byte_array)):
        v = byte_array[i] & 0xFF
        hex_array.append(hex_char[v >> 4])
        hex_array.append(hex_char[v & 0x0F])
    return ''.join(hex_array)


comb = lambda s, n: [s[i:i + n] for i in range(0, len(s), n)]


def mac_fill_up(mac_string):
    mac_array = comb(mac_string, 2)
    return ':'.join(mac_array)


def broadcast2code(broadcast):
    try:
        head1 = unpack('b', binascii.a2b_hex(broadcast[0:2]))[0]
        index1 = 2 + head1 * 2
        head2 = unpack('b', binascii.a2b_hex(broadcast[index1:index1 + 2]))[0]
        index2 = index1 + head2 * 2 + 2
        head3 = unpack('b', binascii.a2b_hex(broadcast[index2:index2 + 2]))[0]
        mu_type = broadcast[index2 + 2:index2 + 4]
        if mu_type == 'FF':
            return broadcast[index2 + 4:index2 + head3 * 2 + 2]
    finally:
        return ""


class ReadMUReportMessage(StreamCallback):
    __source = None
    __header = None
    __pd = None
    __type = None
    __length = None
    __temperature = None
    __humidity = None
    __ap_mac_address = None
    __number_of_mu = None
    __mu = []
    __crc = None
    __time = None
    __access_time = None

    def __init__(self):
        pass

    def get_source(self):
        return self.__source

    def get_header(self):
        return self.__header

    def get_pd(self):
        return self.__pd

    def get_type(self):
        return self.__type

    def get_length(self):
        return self.__length

    def get_temperature(self):
        return self.__temperature

    def get_humidity(self):
        return self.__humidity

    def get_ap_mac_address(self):
        return self.__ap_mac_address

    def get_number_of_mu(self):
        return self.__number_of_mu

    def get_crc(self):
        return self.__crc

    def get_time(self):
        return self.__time

    def get_access_time(self):
        return self.__access_time

    def get_all(self):
        return {
            "header": self.__header,
            "pd": self.__pd,
            "type": self.__type,
            "length": self.__length,
            "temperature": self.__temperature,
            "humidity": self.__humidity,
            "ap_mac_address": self.__ap_mac_address,
            "number_of_mu": self.__number_of_mu,
            "mu": self.__mu,
            "crc": self.__crc,
            "time": self.__time,
            "access_time": self.__access_time
        }

    def process(self, input, output):
        byte_array = IOUtils.toByteArray(input).tolist()
        self.__source = byte_to_hex_string(byte_array)
        self.__header = self.__source[:4]
        self.__pd = self.__source[4:8]
        self.__type = unpack('B', binascii.a2b_hex(self.__source[8:10]))[0]
        self.__length = unpack('H', binascii.a2b_hex(self.__source[10:14]))[0]
        self.__temperature = unpack('h', binascii.a2b_hex(self.__source[14:18]))[0]
        self.__humidity = unpack('h', binascii.a2b_hex(self.__source[18:22]))[0]
        self.__ap_mac_address = mac_fill_up(self.__source[22:34])
        self.__number_of_mu = unpack('H', binascii.a2b_hex(self.__source[34:38]))[0]
        self.__access_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        index = 38
        for i in range(self.__number_of_mu):
            mu_mac = mac_fill_up(self.__source[index:index + 12])
            index += 12
            mu_rssi = unpack('b', binascii.a2b_hex(self.__source[index:index + 2]))[0]
            index += 2
            mu_broadcast = self.__source[index:index + 124]
            index += 124
            self.__mu.append({
                "mac": mu_mac,
                "rssi": mu_rssi,
                "distance": pow(10.0, (abs(mu_rssi) - 50) / (10 * 2.0)),
                "broadcast": mu_broadcast,
                "code": broadcast2code(mu_broadcast)
            })
        self.__crc = self.__source[index:index + 4]
        index += 4
        self.__time = unpack('19s', binascii.a2b_hex(self.__source[index:index + 38]))[0]
        text = json.dumps(self.get_all())
        output.write(text)


flowFile = session.get()
if flowFile is not None:
    try:
        reader = ReadMUReportMessage()
        session.write(flowFile, reader)
        flowFile = session.putAttribute(flowFile, "source", reader.get_source())
        flowFile = session.putAttribute(flowFile, "header", reader.get_header())
        flowFile = session.putAttribute(flowFile, "pd", reader.get_pd())
        flowFile = session.putAttribute(flowFile, "type", str(reader.get_type()))
        flowFile = session.putAttribute(flowFile, "length", str(reader.get_length()))
        flowFile = session.putAttribute(flowFile, "temperature", str(reader.get_temperature()))
        flowFile = session.putAttribute(flowFile, "humidity", str(reader.get_humidity()))
        flowFile = session.putAttribute(flowFile, "ap_mac_address", reader.get_ap_mac_address())
        flowFile = session.putAttribute(flowFile, "number_of_mu", str(reader.get_number_of_mu()))
        flowFile = session.putAttribute(flowFile, "crc", reader.get_crc())
        flowFile = session.putAttribute(flowFile, "time", reader.get_time())
        flowFile = session.putAttribute(flowFile, "access_time", reader.get_access_time())
        session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
    except:
        session.transfer(flowFile, ExecuteScript.REL_FAILURE)
