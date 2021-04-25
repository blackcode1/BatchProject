package BatchDataPacket.DataTypeChange;

import BatchDataPacket.SubClassDataType.LogList;
import BatchDataPacket.DataType;
import BatchDataPacket.SubClassDataType.JsonList;
import StreamProjectInit.StreamLog;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.Random;

public class MapJS2Datatype extends RichMapFunction<String, DataType> {
    public String pid;
    public Boolean isSignal;

    public MapJS2Datatype(String pid) {
        this.pid = pid;
        this.isSignal = false;
    }


    public MapJS2Datatype(String pid, Boolean isSignal) {
        this.pid = pid;
        this.isSignal = isSignal;
    }

    @Override
    public DataType map(String s) throws Exception {
        DataType res = null;

        try {
            if(this.isSignal){
                Integer randDeviceId = new Random().nextInt(100);
                res = (DataType) new JsonList(JSONObject.parseObject(s), randDeviceId.toString());
            }
            else{
                res = (DataType) new JsonList(JSONObject.parseObject(s));
            }
            res.setSignal(this.isSignal);
        }catch (Exception e){
            String logStr = StreamLog.createLocalLog(e, "WARN", "流数据包格式异常，转换成JSONObject时出错", pid);
            res = (DataType) new LogList(logStr);
        }
        return res;
    }
}
