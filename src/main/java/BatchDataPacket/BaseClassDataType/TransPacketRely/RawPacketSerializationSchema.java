package BatchDataPacket.BaseClassDataType.TransPacketRely;

import org.apache.flink.api.common.serialization.SerializationSchema;
import ty.pub.BeanUtil;
import ty.pub.RawDataPacket;

public class RawPacketSerializationSchema implements SerializationSchema<RawDataPacket> {

    @Override
    public byte[] serialize(RawDataPacket rawDataPacket) {
        return BeanUtil.getBytesForRaw(rawDataPacket);
    }
}
