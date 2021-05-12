package cn.edu.thss.rcsdk;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.*;

public class SimpleTimeWindow implements Serializable {

    public List<Tuple2<Long, JSONObject>> windowPackets = new ArrayList<>();
    public Long size;
    public Long slide;
    public Long startTime = System.currentTimeMillis();

    public SimpleTimeWindow(Long size) {
        this.size = size;
        this.slide = size;
    }

    public SimpleTimeWindow(Long size, Long slide) {
        this.size = size;
        this.slide = slide;
    }

    public List<JSONObject> myTimeWindow(JSONObject jsonObject){
        Long curTime = System.currentTimeMillis();
        List<JSONObject> res = new ArrayList<>();
        if(jsonObject != null){
            windowPackets.add(new Tuple2<>(curTime, jsonObject));
        }
        if(curTime > startTime + size){
            for(Tuple2<Long, JSONObject> tuple2 : windowPackets){
                if(tuple2.f0 < startTime + size){
                    res.add(tuple2.f1);
                }
            }
            startTime = startTime + slide;
            Iterator<Tuple2<Long, JSONObject>> iterator = windowPackets.iterator();
            while (iterator.hasNext()){
                Tuple2<Long, JSONObject> tuple2 = iterator.next();
                if(tuple2.f0 < startTime){
                    iterator.remove();
                }
            }
        }
        return res;
    }
}
