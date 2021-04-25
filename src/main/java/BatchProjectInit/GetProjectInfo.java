package BatchProjectInit;

import BatchDataPacket.BaseClass.OCProject;
import com.alibaba.fastjson.JSONObject;

public class GetProjectInfo {


    public static OCProject getProjectInfo(String[] args) throws Exception {
        OCProject project = null;

        try {
            JSONObject projectJson = JSONObject.parseObject(args[0]);
            project = new OCProject(projectJson);
        } catch (Exception e) {
            RunProject.logStr = StreamLog.createExLog(e, "ERROR", "项目信息解析异常");
        }

        return project;
    }
}
