package BatchDataPacket.SubClassDataType;

import BatchDataPacket.DataType;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonList extends DataType {
    //Json:{String dataID, Long timestamp, Map<String gk, Map<Long key, String value>> workStatusMap}
    public JSONObject streamData;

    public JsonList(JSONObject streamData) {
        super("JSONObject");
        this.streamData = streamData;
        if(streamData.containsKey("deviceID")){
            this.dataID = streamData.getString("deviceID");
        }
        else if(streamData.containsKey("car")){
            this.dataID = streamData.getString("car");
        }
        else {
            this.dataID = "0";
        }
    }

    public JsonList(JSONObject streamData, String dataID) {
        super("JSONObject");
        this.streamData = streamData;
        this.dataID = dataID;
    }

    public JsonList(JSONObject streamData, String dataID, String outputTopic) {
        super("JSONObject", outputTopic);
        this.streamData = streamData;
        this.dataID = dataID;
    }

    public JsonList(JSONObject streamData, String dataID, String outputTopic, String intopic) {
        super("JSONObject", outputTopic);
        this.streamData = streamData;
        this.dataID = dataID;
        this.source = intopic;
    }

    public List<String> allGKID()throws Exception{
        if(!this.streamData.containsKey("workStatusMap")){
            return new ArrayList<>();
        }
        Map<String, Map<String, String>> res = (Map<String, Map<String, String>>) this.streamData.get("workStatusMap");
        List<String> gkIDList = new ArrayList<String>(res.keySet());
        return gkIDList;
    }

    public Map<String, Map<String, String>> getWorkStatusMap()throws Exception{
        if(!this.streamData.containsKey("workStatusMap")){
            return new HashMap<>();
        }
        Map<String, Map<String, String>> res = (Map<String, Map<String, String>>) this.streamData.get("workStatusMap");
        return res;
    }

    @Override
    public String toString() {
        return "JsonList{" +
                "streamData=" + streamData +
                ", streamDataType='" + streamDataType + '\'' +
                ", dataID='" + dataID + '\'' +
                ", outputTopic='" + outputTopic + '\'' +
                ", source='" + source + '\'' +
                '}';
    }
}
