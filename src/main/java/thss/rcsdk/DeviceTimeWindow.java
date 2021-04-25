package thss.rcsdk;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeviceTimeWindow implements Serializable {

    public Long size;
    public Long slide;
    Map<String, SimpleTimeWindow> timeWindowMap = new HashMap<>();

    public DeviceTimeWindow(Long size) {
        this.size = size;
        this.slide = size;
    }

    public DeviceTimeWindow(Long size, Long slide) {
        this.size = size;
        this.slide = slide;
    }

    public List<JSONObject> myTimeWindow(JSONObject jsonObject){
        if(jsonObject == null || !jsonObject.containsKey("baseInfoMap") || !jsonObject.getJSONObject("baseInfoMap").containsKey("TrainrNumber")){
            return new ArrayList<>();
        }
        String key = jsonObject.getJSONObject("baseInfoMap").getString("TrainrNumber");
        return myTimeWindow(jsonObject, key);
    }

    public List<JSONObject> myTimeWindow(JSONObject jsonObject, String key){
        if(!timeWindowMap.containsKey(key)){
            timeWindowMap.put(key, new SimpleTimeWindow(size, slide));
        }
        return timeWindowMap.get(key).myTimeWindow(jsonObject);
    }
}
