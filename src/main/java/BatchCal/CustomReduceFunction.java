package BatchCal;

import StreamDataPacket.BaseClassDataType.StreamTask;
import StreamDataPacket.BaseClassDataType.TaskState;
import StreamDataPacket.DataType;
import StreamDataPacket.SubClassDataType.TaskInfoPacket;
import StreamDataPacket.SubClassDataType.TaskVarPacket;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import cn.edu.thss.rcsdk.RealTimeAlg;
import thss.rcsdk.BatchAlg;

import java.util.*;

public class CustomReduceFunction extends RichGroupReduceFunction<JSONObject, JSONObject> {

    private transient Map<String, BatchAlg> userAlg = new HashMap<>();
    private transient TaskInfoPacket  taskInfoPacket = new TaskInfoPacket();
    private transient Map<String, Map<String, List<Map<String, String>>>> allTaskVar;
    public CustomReduceFunction(JSONObject jsonObject) {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // todo 这里需要测试一下
        List<DataType> conf = (List<DataType>) getRuntimeContext().getBroadcastVariable("broadcast").get(0);
        userAlg = new HashMap<>();
        for(DataType dataType : conf){
            if(dataType.streamDataType.equals("taskInfo")){
                taskInfoPacket = (TaskInfoPacket) dataType;
            }else{
                TaskVarPacket taskVarPacket = (TaskVarPacket) dataType;
                allTaskVar.put(dataType.dataID, taskVarPacket.taskvar);
            }
        }


    }

    @Override
    public void reduce(Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {
        Map<String, List<JSONObject>> input = new HashMap<>();
        iterable.forEach(o -> {
            String key = o.getString("xx");
            if(!input.containsKey(key)){
                input.put(o.getString(""), new ArrayList<>());
            }
            input.get(key).add(o);
        });
        for(Map.Entry entry : input.entrySet()){
            for(StreamTask streamTask : taskInfoPacket.taskList){
                BatchAlg batchAlg = GetUserAlg.getUserAlg(userAlg, taskInfoPacket.jarInfoMap, streamTask.jarID);
                Map<String, List<Map<String, String>>> conf = allTaskVar.get(entry.getKey());
                TaskState state = new TaskState();
                for(JSONObject jsonObject : iterable){
                    List<JSONObject> res = batchAlg.callAlg(null, null, (List<JSONObject>)entry.getValue(),
                            null, conf, (String)entry.getKey(), null
                    ,state,0L,0L);
                    for(JSONObject object : res){
                        collector.collect(object);
                    }
                }
            }
        }


    }
}
