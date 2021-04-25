package BatchDataPacket.SubClassDataType;

import BatchDataPacket.BaseClassDataType.JarInfo;
import BatchDataPacket.BaseClassDataType.StreamTask;
import BatchDataPacket.DataType;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//任务信息，包含：
//活跃任务列表List<StreamTask>
//公有状态信息列表：Map<state_id, Map<key, value>>
//算法信息列表Map<jar_id, JarInfo>
//任务输出数据集列表Map<task_id，StreamDataset>
public class TaskInfoPacket extends DataType {
    public List<BatchDataPacket.BaseClassDataType.StreamTask> taskList;
    public Map<String, Map<String, String>> stateInfoList;
    public Map<String, BatchDataPacket.BaseClassDataType.JarInfo> jarInfoMap;

    public String getStateTaskID(String stateID){
        if(!this.stateInfoList.containsKey(stateID)){
            return null;
        }
        return this.stateInfoList.get(stateID).get("TaskID");
    }

    public TaskInfoPacket() {
        super("taskInfo");
        this.taskList = new ArrayList<BatchDataPacket.BaseClassDataType.StreamTask>();
        this.stateInfoList = new HashMap<String, Map<String, String>>();
        this.jarInfoMap = new HashMap<String, BatchDataPacket.BaseClassDataType.JarInfo>();
    }

    public TaskInfoPacket(JSONObject jsonObject) throws Exception{
        super("taskInfo");

        this.taskList = new ArrayList<BatchDataPacket.BaseClassDataType.StreamTask>();
        JSONArray taskListJson = jsonObject.getJSONArray("TaskList");
        for(int i = 0; i < taskListJson.size(); i++){
            JSONObject taskJson = taskListJson.getJSONObject(i);
            this.taskList.add(new StreamTask(taskJson));
        }

        this.stateInfoList = new HashMap<String, Map<String, String>>();
        JSONArray stateInfoListJson = jsonObject.getJSONArray("StateInfoList");
        for(int i = 0; i < stateInfoListJson.size(); i++){
            Map<String, String> stateInfo = new HashMap<String, String>();
            JSONObject stateInfoJson = stateInfoListJson.getJSONObject(i);
            stateInfo.put("StateID", stateInfoJson.getString("StateID"));
            stateInfo.put("TaskID", stateInfoJson.getString("TaskID"));
            this.stateInfoList.put(stateInfoJson.getString("StateID"), stateInfo);
        }

        this.jarInfoMap = new HashMap<String, BatchDataPacket.BaseClassDataType.JarInfo>();
        JSONArray jarInfoMapJson = jsonObject.getJSONArray("JarInfoList");
        for(int i = 0; i < jarInfoMapJson.size(); i++){
            JSONObject jarInfoJson = jarInfoMapJson.getJSONObject(i);
            this.jarInfoMap.put(jarInfoJson.getString("JarID"), new JarInfo(jarInfoJson));
        }
    }

    @Override
    public String toString() {
        return "TaskInfoPacket{" +
                "taskList=" + taskList +
                ", stateInfoList=" + stateInfoList +
                ", jarInfoMap=" + jarInfoMap +
                ", streamDataType='" + streamDataType + '\'' +
                ", dataID='" + dataID + '\'' +
                ", outputTopic='" + outputTopic + '\'' +
                '}';
    }
}
