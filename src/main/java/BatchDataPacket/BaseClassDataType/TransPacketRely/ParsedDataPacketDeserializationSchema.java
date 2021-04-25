package BatchDataPacket.BaseClassDataType.TransPacketRely;

import StreamProjectInit.StreamLog;
import edu.thss.entity.ParsedDataPacket;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class ParsedDataPacketDeserializationSchema implements DeserializationSchema<ParsedDataPacket> {

    private static final long serialVersionUID = 1L;
    private BatchDataPacket.BaseClassDataType.TransPacketRely.ParsedDataPacketDecoder decoder = new ParsedDataPacketDecoder();

    public TypeInformation<ParsedDataPacket> getProducedType() {
        return TypeInformation.of(new TypeHint<ParsedDataPacket>() {
            public TypeInformation<ParsedDataPacket> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
    }

    public ParsedDataPacket deserialize(byte[] arg0) throws IOException {
        ParsedDataPacket transPacket = new ParsedDataPacket();
        try {
            transPacket = decoder.deserialize("", arg0);
        } catch (Exception e) {
            transPacket.setMsgType("@log");
            transPacket.addBaseInfo("err", StreamLog.getExc(e));
            transPacket.setDeviceID(null);
        }
        return transPacket;
    }

    public boolean isEndOfStream(ParsedDataPacket arg0) {
        // TODO Auto-generated method stub
        return false;
    }

}
