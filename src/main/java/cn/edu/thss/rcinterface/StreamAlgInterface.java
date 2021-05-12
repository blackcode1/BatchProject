package cn.edu.thss.rcinterface;

import StreamDataPacket.BaseClassDataType.TaskState;
import com.alibaba.fastjson.JSONObject;
import ty.pub.RawDataPacket;
import ty.pub.TransPacket;

import java.util.List;
import java.util.Map;

public interface StreamAlgInterface<T> {
    public Boolean initAlg(RawDataPacket rawInput, TransPacket transInput, JSONObject jsonInput,
                           List<String> condition, Map<String, List<Map<String, String>>> config, String deviceID,
                           List<TaskState> publicState, TaskState privateState, Long curtime) throws Exception;

    public List<T> callAlg(RawDataPacket rawInput, TransPacket transInput, JSONObject jsonInput,
                           List<String> condition, Map<String, List<Map<String, String>>> config, String deviceID,
                           List<TaskState> publicState, TaskState privateState, Long curtime) throws Exception;
}
