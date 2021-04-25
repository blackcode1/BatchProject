package BatchDataPacket.DataTypeChange;

import BatchDataPacket.DataType;
import org.apache.flink.api.common.functions.MapFunction;

public class ADDDatatyeSource implements MapFunction<DataType, DataType> {

    public String did;
    public Boolean isSignal;

    public ADDDatatyeSource(String id) {
        this.did = id;
        isSignal = false;
    }

    public ADDDatatyeSource(String did, Boolean isSignal) {
        this.did = did;
        this.isSignal = isSignal;
    }

    @Override
    public DataType map(DataType dataType) throws Exception {
        dataType.setSource(did);
        dataType.setSignal(isSignal);
        return dataType;
    }
}
