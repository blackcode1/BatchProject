package cn.edu.thss.rcsdk;

import StreamDataPacket.BaseClassDataType.TaskState;
import com.alibaba.fastjson.JSONObject;
import ty.pub.RawDataPacket;
import ty.pub.TransPacket;

import java.util.List;
import java.util.Map;

public class RTCFInput {
    public RawDataPacket rawInput;
    public TransPacket transInput;
    public JSONObject jsonInput;
    public List<String> condition;
    public Map<String, List<Map<String, String>>> config;
    public List<TaskState> publicState;
    public TaskState privateState;
    public String deviceID;
    public String taskID;

    public RTCFInput(String deviceID, String taskID) {
        this.rawInput = null;
        this.transInput = null;
        this.jsonInput = null;
        this.condition = null;
        this.config = null;
        this.publicState = null;
        this.privateState = null;
        this.deviceID = deviceID;
        this.taskID = taskID;
    }

    public RTCFInput(RawDataPacket rawInput, TransPacket transInput, JSONObject jsonInput, List<String> condition, Map<String, List<Map<String, String>>> config, List<TaskState> publicState, TaskState privateState) {
        this.rawInput = rawInput;
        this.transInput = transInput;
        this.jsonInput = jsonInput;
        this.condition = condition;
        this.config = config;
        this.publicState = publicState;
        this.privateState = privateState;
    }

    public RTCFInput(JSONObject jsonObject){
        this.rawInput = JSONObject.parseObject(jsonObject.getString("rawInput"), RawDataPacket.class);
        this.transInput = JSONObject.parseObject(jsonObject.getString("transInput"), TransPacket.class);
        this.jsonInput = (JSONObject) jsonObject.get("jsonInput");
        this.condition = (List<String>) jsonObject.get("condition");
        this.config = (Map<String, List<Map<String, String>>>) jsonObject.get("taskvar");
        this.publicState = (List<TaskState>) jsonObject.get("publicStateList");
        this.privateState = (TaskState) jsonObject.get("privateState");
        this.deviceID = jsonObject.getString("deviceID");
        this.taskID = jsonObject.getString("taskID");
    }

    @Override
    public String toString() {
        return "RTCFInput{" +
                "\n   rawInput=" + rawInput +
                ", \n   transInput=" + transInput +
                ", \n   jsonInput=" + jsonInput +
                ", \n   condition=" + condition +
                ", \n   config=" + config +
                ", \n   publicState=" + publicState +
                ", \n   privateState=" + privateState +
                ", \n   deviceID='" + deviceID + '\'' +
                ", \n   taskID='" + taskID + '\'' +
                "\n}";
    }

    public String toStringWOConfig() {
        return "RTCFInput{" +
                "\n   rawInput=" + rawInput +
                ", \n   transInput=" + transInput +
                ", \n   jsonInput=" + jsonInput +
                ", \n   condition=" + condition +
                ", \n   publicState=" + publicState +
                ", \n   privateState=" + privateState +
                ", \n   deviceID='" + deviceID + '\'' +
                ", \n   taskID='" + taskID + '\'' +
                "\n}";
    }
}
