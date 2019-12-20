from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import InputStreamCallback, OutputStreamCallback
from java.nio.charset import StandardCharsets
from org.apache.commons.io import IOUtils
import json


class ReadStream(InputStreamCallback):
    __success = False
    __input_json = None

    def __init__(self):
        pass

    def success(self):
        return self.__success

    def get__input_json(self):
        return self.__input_json

    def process(self, input_Stream):
        origin = IOUtils.toString(input_Stream, StandardCharsets.UTF_8)
        self.__input_json = json.loads(origin)
        self.__success = True


class WriteStream(OutputStreamCallback):
    __success = False
    content = None

    def __init__(self, content):
        self.content = content

    def success(self):
        return self.__success

    def process(self, output_Stream):
        output_Stream.write(json.dumps(self.content))
        self.__success = True


flowFile = session.get()
if flowFile is not None:
    reader = ReadStream()
    session.read(flowFile, reader)
    json_in = reader.get__input_json()
    device_mac = flowFile.getAttribute('ap_mac_address')
    data = {
        'devMac': flowFile.getAttribute(''),
        'devCode':flowFile.getAttribute(''),
        'distance':flowFile.getAttribute(''),
        'sendTime':flowFile.getAttribute(''),
        'accessTime':flowFile.getAttribute(''),
        'schoolCode': json_in[device_mac]
    }
    # flowFile1 = session.create(data)
    # flowFile = session.create()
    writer = WriteStream(data)
    session.write(flowFile, writer)
    if reader.success():
        session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
    else:
        session.transfer(flowFile, ExecuteScript.REL_FAILURE)
