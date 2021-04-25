package BatchDataPacket.BaseClassDataType.TransPacketRely;

import org.apache.kafka.common.serialization.Deserializer;
import ty.pub.TransPacket;

import java.io.Serializable;
import java.util.Map;

public class TransPacketDecoder implements Deserializer<TransPacket>, Serializable{
    public TransPacketDecoder() {
    }

    public void close() {
    }

    public void configure(Map<String, ?> arg0, boolean arg1) {
    }

    public TransPacket deserialize(String arg0, byte[] bytes){
        return (TransPacket) BeanUtil.toObject(bytes, TransPacket.class);
    }
}
