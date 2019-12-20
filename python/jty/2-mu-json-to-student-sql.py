from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import StreamCallback
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
import json


class MUToStudentSQL(StreamCallback):
    __sql = None
    __flow_file = None

    def __init__(self, ff):
        self.__flow_file = ff

    def get_source(self):
        return self.__source

    def get_sql(self):
        return self.__sql

    def process(self, input, output):
        text = IOUtils.toString(input, StandardCharsets.UTF_8)
        json_text = json.loads(text)
        self.__sql = "select A.student_id as student_id, " + \
            "B.name as student_name, " + \
            "C.parent_id as parent_id, " + \
            "D.nickname as parent_name, " + \
            "D.phone as parent_phone, " + \
            "D.school_id as school_code " + \
            "from jty_device as A " + \
            "left join jty_student as B on A.student_id = B.id " + \
            "left join jty_student_parent as C on B.id = C.student_id " + \
            "left join jty_user as D on C.parent_id = D.id " + \
            "where A.personal_flag = 1 " + \
            "and A.code = '%s' " % json_text['mac']
        output.write(self.__sql)


flowFile = session.get()
if flowFile is not None:
    reader = MUToStudentSQL(flowFile)
    session.write(flowFile, reader)
    flowFile = session.putAttribute(flowFile, "student.sql", reader.get_sql())
    session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
