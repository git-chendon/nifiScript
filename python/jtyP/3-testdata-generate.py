import json

from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import InputStreamCallback
from org.apache.nifi.processor.io import OutputStreamCallback
from org.apache.nifi.processor.io import StreamCallback
from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.components.state import Scope  # 管理状态


class PyOutStreamCallback(OutputStreamCallback):
    def __init__(self):
        pass

    def process(self, outputStream):
        outputStream.write()


flowFile = session.get()
if flowFile is not None:
    try:
        flowFile = session.write(flowFile, PyOutputStreamCallback())
        session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
    except:
        session.transfer(flowFile, ExecuteScript.REL_FAILURE)

# implicit return at the end
