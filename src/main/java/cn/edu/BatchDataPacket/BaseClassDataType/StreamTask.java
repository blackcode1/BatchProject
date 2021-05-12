package cn.edu.BatchDataPacket.BaseClassDataType;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamTask {

    public String taskID;
    public String jarID;
    public List<String> inputDataSetList;
    public Boolean useOnTimeSource;
    public String outputDataSet;
    public List<String> publicStateIDList;
    public String stateType;
    public Long statePeriod;
    public List<String> condition;
    public List<String> deviceList;
    public Integer taskState;
    public String outputTopic;
    public Integer timeout;
    public List<String> inputTopicTest;

    public StreamTask(JSONObject jsonObject) throws Exception{
        this.taskID = jsonObject.getString("TaskID");
        this.jarID = jsonObject.getString("JarID");
        this.inputDataSetList = new ArrayList<String>(Arrays.asList(
                jsonObject.getString("InputDataSetList").split(",")));
        this.useOnTimeSource = jsonObject.getBoolean("Broadcast");
        this.outputDataSet = jsonObject.getString("OutputDataSet");
        this.publicStateIDList = new ArrayList<String>(Arrays.asList(
                jsonObject.getString("PublicStateIDList").split(",")));
        this.stateType = jsonObject.getString("StateType");
        this.statePeriod = jsonObject.getLong("StatePeriod");
        this.condition = new ArrayList<String>(Arrays.asList(
                jsonObject.getString("Condition").split(",")));
        this.deviceList = new ArrayList<String>(Arrays.asList(
                jsonObject.getString("DeviceList").split(",")));
        this.taskState = jsonObject.getInteger("TaskState");
        this.outputTopic = jsonObject.getString("outputTopic");
        if(jsonObject.containsKey("timeout")){
            this.timeout = jsonObject.getInteger("timeout");
        }
        if(this.timeout == null || this.timeout <= 0){
            this.timeout = 10000;
        }
        this.inputTopicTest = new ArrayList<String>(Arrays.asList(
                jsonObject.getString("inputTopicList").split(",")));
    }

    @Override
    public String toString() {
        return "StreamTask{" +
                "taskID='" + taskID + '\'' +
                ", jarID='" + jarID + '\'' +
                ", inputDataSetList=" + inputDataSetList +
                ", useOnTimeSource=" + useOnTimeSource +
                ", outputDataSet='" + outputDataSet + '\'' +
                ", publicStateIDList=" + publicStateIDList +
                ", stateType='" + stateType + '\'' +
                ", statePeriod=" + statePeriod +
                ", condition=" + condition +
                ", deviceList=" + deviceList +
                ", taskState=" + taskState +
                ", outputTopic='" + outputTopic + '\'' +
                ", timeout=" + timeout +
                ", inputTopicTest=" + inputTopicTest +
                '}';
    }
}
