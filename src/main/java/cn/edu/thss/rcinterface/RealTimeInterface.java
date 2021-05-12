package cn.edu.thss.rcinterface;

import StreamDataPacket.BaseClassDataType.TaskState;
import com.alibaba.fastjson.JSONObject;
import ty.pub.RawDataPacket;
import ty.pub.TransPacket;

import java.util.List;
import java.util.Map;

public interface RealTimeInterface<T> {
    public Boolean init(RawDataPacket rawInput, TransPacket transInput, JSONObject jsonInput,
                        List<String> condition, Map<String, List<Map<String, String>>> config,
                        List<TaskState> publicState, TaskState privateState) throws Exception;

    public List<T> callAlg(RawDataPacket rawInput, TransPacket transInput, JSONObject jsonInput,
                           List<String> condition, Map<String, List<Map<String, String>>> config,
                           List<TaskState> publicState, TaskState privateState) throws Exception;
}
