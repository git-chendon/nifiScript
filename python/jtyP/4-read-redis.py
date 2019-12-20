from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import InputStreamCallback
from org.apache.nifi.processor.io import StreamCallback
from org.apache.nifi.processors.script import ExecuteScript


class PyStreamCallback(StreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        outputStream.write(bytearray('Hello Word!'[::-1].encode('utf-8')))


flowFile = session.get()
service = context.getControllerServiceLookup().getControllerService("f99205eb-016e-1000-25eb-566a38c570f2")
redis = service.getConnection()
redis.close()
if flowFile is not None:
    try:
        myValue2 = myProperty2.evaluateAttributeExpressions(flowFile).getValue()
        session.write(flowFile, PyStreamCallback())
        flowFile = session.putAttribute(flowFile, "source", redis.dbSize())
        session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
    except:
        session.transfer(flowFile, ExecuteScript.REL_FAILURE)