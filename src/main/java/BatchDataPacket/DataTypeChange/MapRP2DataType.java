package BatchDataPacket.DataTypeChange;

import BatchDataPacket.DataType;
import BatchDataPacket.SubClassDataType.RawPacketList;
import org.apache.flink.api.common.functions.MapFunction;
import ty.pub.RawDataPacket;

public class MapRP2DataType implements MapFunction<RawDataPacket, DataType> {

    @Override
    public DataType map(RawDataPacket rawDataPacket) throws Exception {
        DataType res = (DataType) new RawPacketList(rawDataPacket);
        return res;
    }
}
