from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import StreamCallback
from java.nio.charset import StandardCharsets
from org.apache.commons.io import IOUtils


class PyStreamCallback(StreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream, outPutStream):
       data_in = IOUtils.toString(inputStream,StandardCharsets.UTF-8)