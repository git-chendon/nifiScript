from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import InputStreamCallback, OutputStreamCallback
import json


class Write(OutputStreamCallback):
    __content = None

    def __init__(self, content):
        self.__content = content

    def process(self, output):
        output.write(self.__content.encode('utf-8'))


flowFile = session.get()
if flowFile is not None:
    token_json = json.loads(flowFile.getAttribute("token.json"))
    if token_json["resultCode"] != "001":
        session.transfer(flowFile, ExecuteScript.REL_FAILURE)
    else:
        student_json = json.loads(flowFile.getAttribute("student.redis"))
        attendance_time = flowFile.getAttribute("mu.time")
        for parent in student_json["parent_list"]:
            pushData = {
                u"function": u"personSend",
                u"schoolCode": parent["parent_school"].decode('latin1'),
                u"phones": parent["parent_phone"].decode('latin1'),
                u"product": u"ykt",
                u"category": u"1",
                u"message": [
                    {
                        u"title": u"子女考勤打卡",
                        u"content": (u"您的子/女 [%s] 已于 [%s] 到校" % (student_json["student_name"], attendance_time)),
                        u"imageUrl": u"图片地址",
                        u"detailUrl": u"跳转地址"
                    }
                ],
                u"accessCode": token_json["accessCode"].decode('latin1')
            }
            text = json.dumps(pushData, ensure_ascii=False)
            pushFlowFile = session.create()
            session.write(pushFlowFile, Write(text))
            session.putAttribute(pushFlowFile, "mu.time", attendance_time)
            sub_student_redis = student_json
            sub_student_redis["parent_list"] = [parent]
            session.putAttribute(pushFlowFile, "student.redis", json.dumps(sub_student_redis, ensure_ascii=False))
            session.putAttribute(pushFlowFile, "mime.type", "application/json")
            session.transfer(pushFlowFile, ExecuteScript.REL_SUCCESS)
        session.remove(flowFile)
