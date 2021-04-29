package BatchInput.getInputBatch;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class KafkaSource extends RichSourceFunction<JSONObject> {
    Properties props = new Properties();

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        props.setProperty("bootstrap.servers", "192.168.3.32:9092");
        props.put("group.id", "tsx");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    @Override
    public void run(SourceContext<JSONObject>  sourceContext) throws Exception {
        List<JSONObject> list = new ArrayList<>();
        Map<TopicPartition, Long> map = new HashMap<>();
        List<PartitionInfo> flink_order = consumer.partitionsFor("TEST50");
        //从半小时前开始消费
        long fetchDataTime = new Date().getTime() - 1000 * 60 * 60 * 24;
        for (PartitionInfo par : flink_order) {
            map.put(new TopicPartition("teststoreb", par.partition()), fetchDataTime);
        }
        Map<TopicPartition, OffsetAndTimestamp> parMap = consumer.offsetsForTimes(map);
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : parMap.entrySet()) {
            TopicPartition key = entry.getKey();
            OffsetAndTimestamp value = entry.getValue();
            if(value == null){
                continue;
            }
            long offset = value.offset();
            System.out.println(key.partition());
            System.out.println(offset);
            //根据消费里的timestamp确定offset
            if (value != null) {
                consumer.assign(Arrays.asList(key));
                consumer.seek(key, offset);
            }
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                if(record.timestamp() > 1614951256144L){
                    break;
                }
                sourceContext.collect(JSONObject.parseObject(record.value()));
            }
            if(records.count() == 0){
                break;
            }
        }

    }

    @Override
    public void cancel() {
        consumer.close();
    }
}
