package cn.edu.BatchCal;

import StreamDataPacket.BaseClassDataType.StreamTask;
import StreamDataPacket.BaseClassDataType.TaskState;
import StreamDataPacket.DataType;
import StreamDataPacket.SubClassDataType.DBStore.DBStore;
import StreamDataPacket.SubClassDataType.JsonList;
import StreamDataPacket.SubClassDataType.TaskInfoPacket;
import StreamDataPacket.SubClassDataType.TaskVarPacket;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import cn.edu.thss.rcsdk.BatchAlg;

import java.util.*;

public class CustomReduceFunction extends RichGroupReduceFunction<DataType, DataType> {

    private transient Map<String, BatchAlg> userAlg = new HashMap<>();
    private transient TaskInfoPacket  taskInfoPacket = new TaskInfoPacket();
    private transient Map<String, List<Map<String, String>>> allTaskVar = new HashMap<>();
    public CustomReduceFunction(JSONObject jsonObject) {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // todo 这里需要测试一下
        List<DataType> conf = (List<DataType>) getRuntimeContext().getBroadcastVariable("broadcast").get(0);
        System.out.println("test");
        userAlg = new HashMap<>();
        for(DataType dataType : conf){
            if(dataType.streamDataType.equals("taskInfo")){
                taskInfoPacket = (TaskInfoPacket) dataType;
            }else{
                TaskVarPacket taskVarPacket = (TaskVarPacket) dataType;
                allTaskVar= taskVarPacket.taskvar;
            }
        }


    }

    @Override
    public void reduce(Iterable<DataType> iterable, Collector<DataType> collector) throws Exception {
        Map<String, List<JSONObject>> input = new HashMap<>();
        iterable.forEach(o -> {
            JsonList data = (JsonList) o;
            String key = data.streamData.getString("device");
            if(!input.containsKey(key)){
                input.put(data.streamData.getString("device"), new ArrayList<>());
            }
            input.get(key).add(data.streamData);
        });
        for(Map.Entry entry : input.entrySet()){
            for(StreamTask streamTask : taskInfoPacket.taskList){
                BatchAlg batchAlg = GetUserAlg.getUserAlg(userAlg, taskInfoPacket.jarInfoMap, streamTask.jarID);
                Map<String, List<Map<String, String>>> conf = allTaskVar;
                TaskState state = new TaskState();
                try {
                    List<DBStore> res = batchAlg.calc(null, null, (List<JSONObject>)entry.getValue(),
                            null, conf, (String)entry.getKey(), null
                            ,state,0L,0L);
                    for(DBStore object : res){
                        DataType resultData = (DataType) object;
                        collector.collect(resultData);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }


    }
}
