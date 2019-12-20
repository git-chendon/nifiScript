from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import StreamCallback
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
import json


class MURedisValue(StreamCallback):
    __student_id = None
    __student_name = None
    __parent_list = []
    __redis_value = None
    __flow_file = None

    def __init__(self, ff):
        self.__flow_file = ff

    def get_student_id(self):
        return self.__student_id

    def get_student_name(self):
        return self.__student_name

    def get_parent_list(self):
        return self.__parent_list

    def get_redis_value(self):
        return self.__redis_value

    def process(self, input_stream, output_stream):
        text = IOUtils.toString(input_stream, StandardCharsets.UTF_8)
        if len(text) > 4:
            json_text = json.loads(text)
            self.__student_id = json_text[0]['student_id']
            self.__student_name = json_text[0]['student_name']
            for parent in json_text:
                parent_data = {
                    'parent_id': parent['parent_id'],
                    'parent_name': parent['parent_name'],
                    'parent_phone': parent['parent_phone'],
                    'parent_school': parent['school_code']
                }
                self.__parent_list.append(parent_data)
            data = {
                'student_id': self.__student_id,
                'student_name': self.__student_name,
                'parent_list': self.__parent_list
            }
            self.__redis_value = json.dumps(data, ensure_ascii=False)
            output_stream.write(self.__redis_value.encode('utf-8'))


flowFile = session.get()
if flowFile is not None:
    reader = MURedisValue(flowFile)
    session.write(flowFile, reader)
    if reader.get_student_id() is not None:
        flowFile = session.putAttribute(flowFile, "student.id", reader.get_student_id())
        flowFile = session.putAttribute(flowFile, "student.name", reader.get_student_name())
        flowFile = session.putAttribute(flowFile, "student.parent",
                                        json.dumps(reader.get_parent_list(), ensure_ascii=False))
        flowFile = session.putAttribute(flowFile, "student.redis", reader.get_redis_value())
        session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
    else:
        session.transfer(flowFile, ExecuteScript.REL_FAILURE)
