from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import InputStreamCallback
from org.apache.nifi.processor.io import StreamCallback
from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.components.state import Scope   #管理状态


# Define a subclass of InputStreamCallback for use in session.read()
class PyInputStreamCallback(InputStreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)


# Do something with text here


class PyStreamCallback(StreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        outputStream.write(bytearray('Hello Word!'[::-1].encode('utf-8')))


flowFile = session.get()
stateManager = context.stateManager
stateMap = stateManager.getState(Scope.CLUSTER)
newMap = {'myKey1': 'myValue1'}
if stateMap.version == -1:
    stateManager.setState(newMap, Scope.CLUSTER)
    context.stateManager.clear(Scope.CLUSTER)
else:
    stateManager.replace(stateMap, newMap, Scope.CLUSTER)

service = context.getControllerServiceLookup().getControllerService('f99205eb-016e-1000-25eb-566a38c570f2')
redis = service.getConnection()

if flowFile is not None:
    try:
        myValue2 = myProperty2.evaluateAttributeExpressions(flowFile).getValue()
        session.write(flowFile, PyStreamCallback())
        oldMap = context.stateManager.getState(Scope.LOCAL).toMap() #获取状态
        session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
    except:
        log.error('出错了')
        session.transfer(flowFile, ExecuteScript.REL_FAILURE)
