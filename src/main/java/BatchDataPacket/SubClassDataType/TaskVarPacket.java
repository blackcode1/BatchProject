package BatchDataPacket.SubClassDataType;

import BatchDataPacket.DataType;

import java.util.List;
import java.util.Map;

public class TaskVarPacket extends DataType {
    public Map<String, List<Map<String, String>>> taskvar;

    public TaskVarPacket(Map<String, List<Map<String, String>>> taskvar) {
        super("taskVarBig");
        this.taskvar = taskvar;
    }

    public TaskVarPacket(Map<String, List<Map<String, String>>> taskvar, String filter) {
        super("taskVarSmall");
        this.taskvar = taskvar;
        this.dataID = filter;
    }

    @Override
    public String toString() {
        return "TaskVarPacket{" +
                "taskvar=" + taskvar +
                ", streamDataType='" + streamDataType + '\'' +
                ", dataID='" + dataID + '\'' +
                ", outputTopic='" + outputTopic + '\'' +
                '}';
    }
}
