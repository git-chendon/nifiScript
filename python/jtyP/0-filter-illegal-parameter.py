import json

from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import InputStreamCallback
from org.apache.nifi.processor.io import StreamCallback
from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.components.state import Scope  # 管理状态


class PyStreamCallback(StreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        json_text = json.loads(text)
        dev_code = json_text['devCode']
        if len(dev_code) is 32:
            outputStream.write(inputStream)


flowFile = session.get()
if flowFile is not None:
    try:
        session.write(flowFile, PyStreamCallback())
        session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
    except:
        session.transfer(flowFile, ExecuteScript.REL_FAILURE)
