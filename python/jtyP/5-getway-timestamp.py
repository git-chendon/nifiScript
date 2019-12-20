from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import StreamCallback
from java.nio.charset import StandardCharsets
from org.apache.commons.io import IOUtils
import json
import time


class ReadMUReportMessage(StreamCallback):
    __success = False
    __ap_mac_address = None
    __time = None

    def __init__(self):
        pass

    def success(self):
        return self.__success

    def get_all(self):
        return {
            "ap_mac_address": self.__ap_mac_address,
            "time": self.__time
        }

    def process(self, input_Stream, output_Stream):
        origin = IOUtils.toString(input_Stream, StandardCharsets.UTF_8)
        origin_json = json.loads(origin)
        # origin_json = json.loads(origin[origin.index('{'):])
        self.__ap_mac_address = origin_json['firstName']
        self.__time = '12.12'
        output_Stream.write(json.dumps(self.get_all()).encode('utf-8'))
        self.__success = True


flowFile = session.get()
if flowFile is not None:
    reader = ReadMUReportMessage()
    session.write(flowFile, reader)
    flowFile = session.putAttribute(flowFile, "time", time.time().__str__())
    if reader.success():
        session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
    else:
        session.transfer(flowFile, ExecuteScript.REL_FAILURE)
