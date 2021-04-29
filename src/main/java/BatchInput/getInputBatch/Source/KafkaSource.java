package BatchInput.getInputBatch.Source;

import StreamDataPacket.BaseClassDataType.TransPacketRely.*;
import StreamDataPacket.DataTypeChange.*;
import StreamDataPacket.DataType;
import edu.thss.entity.ParsedDataPacket;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import ty.pub.RawDataPacket;
import ty.pub.RawPacketDecoder;
import ty.pub.TransPacket;

import java.util.Properties;

public class KafkaSource {

    public static DataStream<DataType> getKafkaStream
            (StreamExecutionEnvironment env, String dataType, String ip, String port, String gropID, String topicName
                    , String pid, String datasetID, Integer offset){
        if(dataType.equals("JSONObject")){
            return env.addSource(getKafkaJsonConsumer(ip, port, gropID, topicName, offset))
                    .map(new MapJS2Datatype(pid))
                    .map(new ADDDatatyeSource(datasetID));
        }
        else if(dataType.equals("TransPacket")){
            return env.addSource(getKafkaTranspacketConsumer(ip, port, gropID, topicName, offset))
                    .map(new MapTP2DataType(pid))
                    .map(new ADDDatatyeSource(datasetID));
        }
        else if(dataType.equals("RawDataPacket")){
            return env.addSource(getKafkaRawpacketConsumer(ip, port, gropID, topicName, offset))
                    .map(new MapRP2DataType())
                    .map(new ADDDatatyeSource(datasetID));
        }
        else if(dataType.equals("ParsedDataPacket")){
            return env.addSource(getKafkaParsedDataPacketConsumer(ip, port, gropID, topicName, offset))
                    .map(new MapPDP2DataType(pid))
                    .map(new ADDDatatyeSource(datasetID));
        }
        return null;
    }

    public static FlinkKafkaConsumer<String> getKafkaJsonConsumer(String ip, String port, String gropID, String topicName, Integer offset) {
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", ip+":"+port);
        kafkaConfig.setProperty("group.id", gropID);
        kafkaConfig.setProperty("default.api.timeout.ms", "5000");
        kafkaConfig.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(topicName, new SimpleStringSchema(), kafkaConfig);
        if(offset != null && (offset == 0 || offset == 2)){
            kafkaConsumer.setStartFromEarliest();
        }
        else if(offset != null && offset == 1){
            kafkaConsumer.setStartFromLatest();
        }
        return kafkaConsumer;
    }

    public static FlinkKafkaConsumer<TransPacket> getKafkaTranspacketConsumer(String ip, String port, String gropID, String topicName, Integer offset) {
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", ip+":"+port);
        kafkaConfig.setProperty("group.id", gropID);
        kafkaConfig.setProperty("default.api.timeout.ms", "5000");
        kafkaConfig.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.setProperty("value.deserializer", TransPacketDecoder.class.getName());
        FlinkKafkaConsumer<TransPacket> kafkaConsumer = new FlinkKafkaConsumer<TransPacket>(topicName, new TransPacketDeserializationSchema(), kafkaConfig);
        if(offset != null && offset == 0){
            kafkaConsumer.setStartFromEarliest();
        }
        else if(offset != null && offset == 1){
            kafkaConsumer.setStartFromLatest();
        }
        return kafkaConsumer;
    }

    public static FlinkKafkaConsumer<RawDataPacket> getKafkaRawpacketConsumer(String ip, String port, String gropID, String topicName, Integer offset){
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", ip+":"+port);
        kafkaConfig.setProperty("group.id", gropID);
        kafkaConfig.setProperty("default.api.timeout.ms", "5000");
        kafkaConfig.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.setProperty("value.deserializer", RawPacketDecoder.class.getName());
        FlinkKafkaConsumer<RawDataPacket> kafkaConsumer = new FlinkKafkaConsumer<RawDataPacket>(topicName, new RawPacketDeserializationSchema(), kafkaConfig);
        if(offset != null && offset == 0){
            kafkaConsumer.setStartFromEarliest();
        }
        else if(offset != null && offset == 1){
            kafkaConsumer.setStartFromLatest();
        }
        return kafkaConsumer;
    }

    public static FlinkKafkaConsumer<ParsedDataPacket> getKafkaParsedDataPacketConsumer(String ip, String port, String gropID, String topicName, Integer offset) {
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", ip+":"+port);
        kafkaConfig.setProperty("group.id", gropID);
        kafkaConfig.setProperty("default.api.timeout.ms", "5000");
        kafkaConfig.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.setProperty("value.deserializer", ParsedDataPacketDecoder.class.getName());
        FlinkKafkaConsumer<ParsedDataPacket> kafkaConsumer = new FlinkKafkaConsumer<ParsedDataPacket>(topicName, new ParsedDataPacketDeserializationSchema(), kafkaConfig);
        if(offset != null && offset == 0){
            kafkaConsumer.setStartFromEarliest();
        }
        else if(offset != null && offset == 1){
            kafkaConsumer.setStartFromLatest();
        }
        return kafkaConsumer;
    }

}
