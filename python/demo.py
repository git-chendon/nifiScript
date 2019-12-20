from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import StreamCallback
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
import json
import base64
import copy


class JtyBand(StreamCallback):
    __success = False
    __id = None
    __timestamp = None
    __flow_file = None

    def __init__(self, ff):
        self.__flow_file = ff

    def success(self):
        return self.__success

    def get_timestamp(self):
        return self.__timestamp

    def get_id(self):
        return self.__id

    @staticmethod
    def get_data_detail(b64str):
        """
        :type b64str: str
        """
        numbers = [ord(x) if type(x) == str else x for x in base64.b64decode(b64str)]
        data = []
        length = len(numbers)
        if length % 6 != 0:
            return "error data, not 6 char"
        i = 0
        while i < length:
            data.append({
                "step": (numbers[i] << 24) + (numbers[i + 1] << 16) + (numbers[i + 2] << 8) + numbers[i + 3],
                "status": numbers[i + 4],
                "heart": numbers[i + 5]
            })
            i += 6
        return data

    def process(self, input_stream, output_stream):
        text = IOUtils.toString(input_stream, StandardCharsets.UTF_8)
        origin = json.loads(text[text.index("{"):])
        self.__id = origin["ID"]
        self.__timestamp = origin["STMP"]
        analysis = copy.deepcopy(origin)
        analysis["DATA"] = self.get_data_detail(origin["DATA"])
        data = {
            "origin": origin,
            "timestamp": self.__timestamp,
            "id": self.__id,
            "analysis": analysis
        }
        output_stream.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
        self.__success = True


flowFile = session.get()
if flowFile is not None:
    reader = JtyBand(flowFile)
    session.write(flowFile, reader)
    flowFile = session.putAttribute(flowFile, "data.id", str(reader.get_id()))
    flowFile = session.putAttribute(flowFile, "data.timestamp", str(reader.get_timestamp()))
    flowFile = session.putAttribute(flowFile, "mime.type", "application/json")
    if reader.success():
        session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
    else:
        session.transfer(flowFile, ExecuteScript.REL_FAILURE)
