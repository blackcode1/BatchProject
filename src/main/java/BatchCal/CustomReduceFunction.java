package BatchCal;

import StreamDataPacket.BaseClassDataType.TaskState;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import cn.edu.thss.rcsdk.RealTimeAlg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomReduceFunction extends RichGroupReduceFunction<JSONObject, JSONObject> {

    private transient Map<String, RealTimeAlg> userAlg = new HashMap<>();
    private transient Map<String, Map<String, List<Map<String, String>>>> allTaskVar = new HashMap<>();

    public CustomReduceFunction(JSONObject jsonObject) {
        RealTimeAlg realTimeAlg = new simplefailurediag();
        userAlg.put("xx", realTimeAlg);

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Map<String, List<Map<String, String>>> conf = (Map<String, List<Map<String, String>>>) getRuntimeContext().getBroadcastVariable("broadcast").get(0);
        allTaskVar = new HashMap<>();
        userAlg = new HashMap<>();
        allTaskVar.put("xx", conf);
        RealTimeAlg realTimeAlg = new simplefailurediag();
        userAlg.put("xx", realTimeAlg);
    }

    @Override
    public void reduce(Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {
        RealTimeAlg rti = userAlg.get("xx");
        Map<String, List<Map<String, String>>> conf = allTaskVar.get("xx");
        TaskState state = new TaskState();
        for(JSONObject jsonObject : iterable){
            List<JSONObject> res = rti.callAlg(null, null, jsonObject, null, conf, null, state);
            for(JSONObject object : res){
                collector.collect(object);
            }
        }
    }
}
