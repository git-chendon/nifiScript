from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import StreamCallback
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
import json


class MURedisValue(StreamCallback):
    __sql = None
    __flow_file = None

    def __init__(self, ff):
        self.__flow_file = ff

    def get_sql(self):
        return self.__sql

    def process(self, input_stream, output_stream):
        text = IOUtils.toString(input_stream, StandardCharsets.UTF_8)
        student_redis = json.loads(self.__flow_file.getAttribute('student.redis'))
        attendance_time = self.__flow_file.getAttribute('mu.time')
        self.__sql = "insert into jty_student_attendance(`student_id`, `attendance_date`) values('%s', '%s')" % \
                     (student_redis['student_id'], attendance_time)
        output_stream.write(self.__sql.encode('utf-8'))


flowFile = session.get()
if flowFile is not None:
    reader = MURedisValue(flowFile)
    session.write(flowFile, reader)
    if reader.get_sql() is not None:
        flowFile = session.putAttribute(flowFile, "attendance.sql", reader.get_sql())
        session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
    else:
        session.transfer(flowFile, ExecuteScript.REL_FAILURE)



