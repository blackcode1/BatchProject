package BatchSink;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class KafkaFormat extends RichOutputFormat {
    Logger logger= LoggerFactory.getLogger(KafkaFormat.class);
    private String servers;
    private String topic;
    private String msgType;
    private Producer<String, String> producer;

    @Override
    public void configure(Configuration configuration) {
        servers=configuration.getString("servers","");
        topic=configuration.getString("topic","");
        this.msgType=configuration.getString("msgType","");

    }

    @Override
    public void open(int i, int i1) throws IOException {
        getKafkaProducer();
    }

    @Override
    public void writeRecord(Object o) throws IOException {
        if (this.msgType.equals(value.getMsgType())){
            ProducerRecord<String, String> record=null;
            if (value.getDeviceID()!=null){
                record = new ProducerRecord<>(topic, value.getDeviceID(), value.toString());
            }else {
                record = new ProducerRecord<>(topic,value.toString());
            }
            producer.send(record);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.producer!=null){
            producer.flush();
            this.producer.close();
        }
    }

    private void getKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.servers);
        props.put("acks", "0");//判断是否成功发送，如果设置为0，那么生产者将不等待任何消息确认，acks=1，这意味着leader写入消息到本地日志就立即响应，而不等待所有follower应答，acks=all 这意味着leader将等待所有副本同步后应答消息。
        props.put("retries", "10");//请求失败生产者重试次数
        props.put("batch.size", "16384");//缓存大小
        props.put("linger.ms", "0");//生产者发送请求之前等待一段时间
        props.put("buffer.memory", "33554432");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        if (this.producer == null) {
            this.producer = new KafkaProducer<>(props);
        }
    }
}
