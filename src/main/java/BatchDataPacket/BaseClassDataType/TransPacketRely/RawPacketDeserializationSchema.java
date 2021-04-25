package BatchDataPacket.BaseClassDataType.TransPacketRely;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import ty.pub.RawDataPacket;
import ty.pub.RawPacketDecoder;

import java.io.IOException;

public class RawPacketDeserializationSchema implements DeserializationSchema<RawDataPacket> {

    private static final long serialVersionUID = 1L;
    private RawPacketDecoder decoder = new RawPacketDecoder();

    public TypeInformation<RawDataPacket> getProducedType() {
        return TypeInformation.of(new TypeHint<RawDataPacket>() {
            public TypeInformation<RawDataPacket> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
    }

    public RawDataPacket deserialize(byte[] arg0) throws IOException {
        return decoder.deserialize("", arg0);
    }

    public boolean isEndOfStream(RawDataPacket arg0) {
        // TODO Auto-generated method stub
        return false;
    }

}
