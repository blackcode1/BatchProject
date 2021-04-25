package BatchDataPacket.DataTypeChange;

import BatchDataPacket.SubClassDataType.LogList;
import BatchDataPacket.DataType;
import BatchDataPacket.SubClassDataType.TransPacketList;
import StreamProjectInit.StreamLog;
import org.apache.flink.api.common.functions.MapFunction;
import ty.pub.TransPacket;

public class MapTP2DataType implements MapFunction<TransPacket, DataType> {
    public String pid;

    public MapTP2DataType(String pid) {
        this.pid = pid;
    }

    @Override
    public DataType map(TransPacket transPacket) throws Exception {
        DataType res = null;
        if(transPacket.getMsgType() != null && transPacket.getMsgType().equals("@log")){
            String logStr = StreamLog.createLog("WARN", "流数据包格式异常，转换成TransPacket时出错",
                    transPacket.getBaseInfoMap().get("err"), pid);
            res = (DataType) new LogList(logStr);
        }
        else {
            res = (DataType) new TransPacketList(transPacket);
        }
        return res;
    }
}
