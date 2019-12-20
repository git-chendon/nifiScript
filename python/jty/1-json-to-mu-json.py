from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import InputStreamCallback, OutputStreamCallback
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
import json


class ReadMUList(InputStreamCallback):
    __mu = []
    __time = None

    def __init__(self):
        pass

    def get_mu(self):
        return self.__mu

    def get_time(self):
        return self.__time

    def process(self, input):
        text = IOUtils.toString(input, StandardCharsets.UTF_8)
        json_text = json.loads(text)
        self.__time = json_text['time']
        self.__mu = json_text['mu']


class WriteMU(OutputStreamCallback):
    __content = None

    def __init__(self, content):
        self.__content = content

    def process(self, output):
        output.write(bytearray(self.__content.encode('utf-8')))


flowFile = session.get()
if flowFile is not None:
    reader = ReadMUList()
    session.read(flowFile, reader)
    for mu in reader.get_mu():
        data = {
            'time': reader.get_time(),
            'rssi': mu['rssi'],
            'mac': mu['mac']
        }
        text = json.dumps(data)
        muFlowFile = session.create()
        session.write(muFlowFile, WriteMU(text))
        session.putAttribute(muFlowFile, "mu.time", reader.get_time())
        session.putAttribute(muFlowFile, "mu.rssi", str(mu['rssi']))
        session.putAttribute(muFlowFile, "mu.mac", mu['mac'])
        session.transfer(muFlowFile, ExecuteScript.REL_SUCCESS)
    session.remove(flowFile)
