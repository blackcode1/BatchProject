package cn.edu.thss.rcinterface;

import StreamDataPacket.BaseClassDataType.TaskState;
import com.alibaba.fastjson.JSONObject;
import ty.pub.RawDataPacket;
import ty.pub.TransPacket;

import java.util.List;
import java.util.Map;

public interface BatchAlgInterface<T> {
    public Boolean initAlg(List<RawDataPacket> rawInput, List<TransPacket> transInput, List<JSONObject> jsonInput,
                           List<String> condition, Map<String, List<Map<String, String>>> config, String deviceID,
                           List<TaskState> publicState, TaskState privateState, Long starttime, Long endtime) throws Exception;

    public List<T> callAlg(List<RawDataPacket> rawInput, List<TransPacket> transInput, List<JSONObject> jsonInput,
                           List<String> condition, Map<String, List<Map<String, String>>> config, String deviceID,
                           List<TaskState> publicState, TaskState privateState, Long starttime, Long endtime) throws Exception;
}
