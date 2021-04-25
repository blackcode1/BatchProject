package BatchDataPacket.BaseClassDataType.TransPacketRely;

import StreamProjectInit.StreamLog;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import ty.pub.TransPacket;

import java.io.IOException;

public class TransPacketDeserializationSchema implements DeserializationSchema<TransPacket> {

    private static final long serialVersionUID = 1L;
    private TransPacketDecoder decoder = new TransPacketDecoder();

    public TypeInformation<TransPacket> getProducedType() {
        return TypeInformation.of(new TypeHint<TransPacket>() {
            public TypeInformation<TransPacket> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
    }

    public TransPacket deserialize(byte[] arg0) throws IOException {
        TransPacket transPacket = new TransPacket();
        try {
            transPacket = decoder.deserialize("", arg0);
        } catch (Exception e) {
            transPacket.setMsgType("@log");
            transPacket.addBaseInfo("err", StreamLog.getExc(e));
            transPacket.setDeviceId(null);
        }
        return transPacket;
    }

    public boolean isEndOfStream(TransPacket arg0) {
        // TODO Auto-generated method stub
        return false;
    }

}
