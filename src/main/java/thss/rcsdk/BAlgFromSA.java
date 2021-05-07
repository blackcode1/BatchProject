package thss.rcsdk;

import StreamDataPacket.BaseClassDataType.TaskState;
import com.alibaba.fastjson.JSONObject;
import ty.pub.RawDataPacket;
import ty.pub.TransPacket;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BAlgFromSA<T> extends BatchAlg<T> {

    private StreamAlg<T> streamAlg;

    public BAlgFromSA(StreamAlg<T> streamAlg) {
        this.streamAlg = streamAlg;
    }

    @Override
    public Boolean init(List<RawDataPacket> rawInput, List<TransPacket> transInput, List<JSONObject> jsonInput, List<String> condition, Map<String, List<Map<String, String>>> config, String deviceID, List<TaskState> publicState, TaskState privateState, Long starttime, Long endtime) throws Exception {
        return true;
    }

    @Override
    public List<T> calc(List<RawDataPacket> rawInput, List<TransPacket> transInput, List<JSONObject> jsonInput, List<String> condition, Map<String, List<Map<String, String>>> config, String deviceID, List<TaskState> publicState, TaskState privateState, Long starttime, Long endtime) throws Exception {
        List<T> res = new ArrayList<>();
        if(rawInput != null){
            for(int i = 0; i < rawInput.size(); i++){
                res.addAll(streamAlg.callAlg(rawInput.get(i), null, null, condition, config, deviceID, publicState, privateState, System.currentTimeMillis()));
            }
            return res;
        }
        else if(transInput != null){
            for(int i = 0; i < transInput.size(); i++){
                TransPacket transPacket = transInput.get(i);
                Long time = transPacket.getTimestamp();
                res.addAll(streamAlg.callAlg(null, transPacket, null, condition, config, deviceID, publicState, privateState, time));
            }
            return res;
        }
        else {
            for(int i = 0; i < jsonInput.size(); i++){
                JSONObject jsonObject = jsonInput.get(i);
                Long time = jsonObject.getLong("timestamp");
                res.addAll(streamAlg.callAlg(null, null, jsonInput.get(i), condition, config, deviceID, publicState, privateState, time));
            }
            return res;
        }
    }
}
