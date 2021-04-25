package BatchDataPacket.SubClassDataType;

import BatchDataPacket.DataType;
import edu.thss.entity.ParsedDataPacket;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParsedDataPacketList extends DataType {
    public ParsedDataPacket streamData;

    public ParsedDataPacketList(ParsedDataPacket streamData) {
        super("ParsedDataPacket");
        this.streamData = streamData;
        this.dataID = streamData.getBaseInfoMap().get("TrainrNumber");
    }

    public ParsedDataPacketList(ParsedDataPacket streamData, String dataID) {
        super("ParsedDataPacket");
        this.streamData = streamData;
        this.dataID = dataID;
    }

    public ParsedDataPacketList(ParsedDataPacket streamData, String dataID, String outputTopic) {
        super("ParsedDataPacket", outputTopic);
        this.streamData = streamData;
        this.dataID = dataID;
    }

    public List<String> allGKID()throws Exception{
        Map<String, Map<Long, String>> res = this.streamData.getWorkStatusMap();
        List<String> gkIDList = new ArrayList<String>(res.keySet());
        return gkIDList;
    }

    @Override
    public String toString() {
        return "ParsedDataPacketList{" +
                "streamData=" + streamData +
                ", streamDataType='" + streamDataType + '\'' +
                ", dataID='" + dataID + '\'' +
                ", outputTopic='" + outputTopic + '\'' +
                ", source='" + source + '\'' +
                ", isSignal=" + isSignal +
                '}';
    }
}
