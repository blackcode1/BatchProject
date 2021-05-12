package cn.edu.BatchDataPacket;

public class DataType implements Cloneable {
    public String streamDataType;
    public String dataID;
    public String outputTopic = null;
    public String source;
    public Boolean isSignal = false;

    public DataType(String streamDataType, String outputTopic) {
        this.streamDataType = streamDataType;
        this.outputTopic = outputTopic;
    }

    public DataType(String streamDataType) {
        this.streamDataType = streamDataType;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setSignal(Boolean signal) {
        isSignal = signal;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        return "DataType{" +
                "streamDataType='" + streamDataType + '\'' +
                ", dataID='" + dataID + '\'' +
                ", outputTopic='" + outputTopic + '\'' +
                ", source='" + source + '\'' +
                '}';
    }
}

