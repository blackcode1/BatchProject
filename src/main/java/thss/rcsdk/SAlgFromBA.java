package thss.rcsdk;

import StreamDataPacket.BaseClassDataType.TaskState;
import cn.edu.thss.rcinterface.StreamAlgInterface;
import com.alibaba.fastjson.JSONObject;
import ty.pub.RawDataPacket;
import ty.pub.TransPacket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SAlgFromBA<T> extends StreamAlg<T> {

    private BatchAlg<T> batchAlg;
    private Long timeWindow;
    private Long startTime;
    public Map<String, List<RawDataPacket>> rawInputs = new HashMap<>();
    public Map<String, List<TransPacket>> transInputs = new HashMap<>();
    public Map<String, List<JSONObject>> jsonInputs = new HashMap<>();

    public SAlgFromBA(BatchAlg<T> batchAlg, Long timeWindow) {
        this.batchAlg = batchAlg;
        this.timeWindow = timeWindow;
        this.startTime = -1L;
    }

    @Override
    public Boolean init(RawDataPacket rawInput, TransPacket transInput, JSONObject jsonInput, List<String> condition, Map<String, List<Map<String, String>>> config, String deviceID, List<TaskState> publicState, TaskState privateState, Long curtime) throws Exception {
        return true;
    }

    @Override
    public List<T> calc(RawDataPacket rawInput, TransPacket transInput, JSONObject jsonInput, List<String> condition, Map<String, List<Map<String, String>>> config, String deviceID, List<TaskState> publicState, TaskState privateState, Long curtime) throws Exception {
        List<T> res = new ArrayList<>();
        if(rawInput != null){
            if(!this.rawInputs.containsKey(deviceID)){
                this.rawInputs.put(deviceID, new ArrayList<>());
            }
            if(this.startTime != -1 && curtime >= this.startTime + timeWindow){
                res = batchAlg.callAlg(this.rawInputs.get(deviceID), null, null, condition, config, deviceID, publicState, privateState, startTime, this.startTime + timeWindow);
                this.rawInputs.put(deviceID, new ArrayList<>());
            }
            this.rawInputs.get(deviceID).add(rawInput);
        }
        else if(transInput != null){
            if(!this.transInputs.containsKey(deviceID)){
                this.transInputs.put(deviceID, new ArrayList<>());
            }
            if(this.startTime != -1 && curtime >= this.startTime + timeWindow){
                res = batchAlg.callAlg(null, this.transInputs.get(deviceID), null, condition, config, deviceID, publicState, privateState, startTime, this.startTime + timeWindow);
                this.transInputs.put(deviceID, new ArrayList<>());
            }
            this.transInputs.get(deviceID).add(transInput);
        }
        else {
            if(!this.jsonInputs.containsKey(deviceID)){
                this.jsonInputs.put(deviceID, new ArrayList<>());
            }
            if(this.startTime != -1 && curtime >= this.startTime + timeWindow){
                res = batchAlg.callAlg(null, null, this.jsonInputs.get(deviceID), condition, config, deviceID, publicState, privateState, startTime, this.startTime + timeWindow);
                this.jsonInputs.put(deviceID, new ArrayList<>());
            }
            this.jsonInputs.get(deviceID).add(jsonInput);
        }
        if(this.startTime == -1){
            this.startTime = curtime;
        }
        else if(curtime >= this.startTime + timeWindow){
            this.startTime = this.startTime + timeWindow;
        }
        return res;
    }
}
