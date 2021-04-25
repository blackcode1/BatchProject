package BatchDataPacket.DataTypeChange;

import BatchDataPacket.SubClassDataType.LogList;
import BatchDataPacket.DataType;
import BatchDataPacket.SubClassDataType.ParsedDataPacketList;
import StreamProjectInit.StreamLog;
import edu.thss.entity.ParsedDataPacket;
import org.apache.flink.api.common.functions.MapFunction;

public class MapPDP2DataType implements MapFunction<ParsedDataPacket, DataType> {
    public String pid;

    public MapPDP2DataType(String pid) {
        this.pid = pid;
    }

    @Override
    public DataType map(ParsedDataPacket transPacket) throws Exception {
        DataType res = null;
        if(transPacket.getMsgType() != null && transPacket.getMsgType().equals("@log")){
            String logStr = StreamLog.createLog("WARN", "流数据包格式异常，转换成TransPacket时出错",
                    transPacket.getBaseInfoMap().get("err"), pid);
            res = (DataType) new LogList(logStr);
        }
        else {
            res = (DataType) new ParsedDataPacketList(transPacket);
        }
        return res;
    }
}
