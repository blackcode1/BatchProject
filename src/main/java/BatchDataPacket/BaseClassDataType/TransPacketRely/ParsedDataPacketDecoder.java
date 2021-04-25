package BatchDataPacket.BaseClassDataType.TransPacketRely;



import edu.thss.entity.ParsedDataPacket;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.Map;

public class ParsedDataPacketDecoder implements Deserializer<ParsedDataPacket>, Serializable{
    public ParsedDataPacketDecoder() {
    }

    public void close() {
    }

    public void configure(Map<String, ?> arg0, boolean arg1) {
    }

    public ParsedDataPacket deserialize(String arg0, byte[] bytes){
        return (ParsedDataPacket) BeanUtil.toObject(bytes, ParsedDataPacket.class);
    }
}
