package BatchDataPacket.SubClassDataType;

import BatchDataPacket.DataType;

public class LogList extends DataType {
    public String logStr;

    public LogList(String logStr) {
        super("Log");
        this.logStr = logStr;
        this.dataID = String.valueOf(System.currentTimeMillis() % 100);
    }

    @Override
    public String toString() {
        return "LogList{" +
                "logStr='" + logStr + '\'' +
                ", streamDataType='" + streamDataType + '\'' +
                ", dataID='" + dataID + '\'' +
                ", outputTopic='" + outputTopic + '\'' +
                '}';
    }
}
