package BatchProjectInit;

import BatchCal.BatchCal;
import BatchDataPacket.BaseClass.DataSet;
import BatchDataPacket.BaseClass.OCProject;
import BatchSink.KafkaFormat;
import BatchCal.CustomReduceFunction;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import BatchInput.getInputBroadcast.BroadcastSource;

import java.util.*;
public class RunProject {
    public static String logStr;


    public static void main(String[] args) throws Exception {

        JSONObject confJSON = new JSONObject();
        OCProject ocProject = GetProjectInfo.getProjectInfo(args);

        ExecutionEnvironment  env = ExecutionEnvironment .getExecutionEnvironment();
        SetStreamEnv.setParaCheckPoint(env, ocProject);

        List<DataSource<JSONObject>> sourceList = new ArrayList<>();
        List<JSONObject> list = new ArrayList<>();
        DataSource<JSONObject> source1 = null;
        for(DataSet<JSON> dataSet:ocProject.inputDataSetList){
            if (dataSet.dataSourceType.equals("KAFKA")){
                Properties props = new Properties();
                props.setProperty("bootstrap.servers", "192.168.3.32:9092");
                props.put("group.id", "tsx");
                props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

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
                        list.add(JSONObject.parseObject(record.value()));
                    }
                    if(records.count() == 0){
                        break;
                    }
                }

                source1 = env.fromCollection(list);
            }
            else if(dataSet.dataSourceType.equals("IOTDB")){
                DataSource<Row> input = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("org.apache.iotdb.jdbc.IoTDBDriver")
                .setDBUrl("jdbc:iotdb://" + dataSet.dataSourceIp + ":" + dataSet.dataSourcePort + "/")
                .setUsername(dataSet.dataSourceUser)
                .setPassword(dataSet.dataSourcePassword)
                .setQuery(dataSet.DataSetQuery)
                .finish());
            }else{

            }
        }

        DataSource<Map<String, List<Map<String, String>>>> broadcast = env.fromCollection(new BroadcastSource("AC03E0AF43604B4D9F027CE77E18315E", "http://192.168.3.32:8000").getDE());

        broadcast.print();


        source1.groupBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                Integer idKey = jsonObject.getString("car").hashCode() % 100;
                if(StringUtils.isNumeric(jsonObject.getString("car"))) {
                    idKey = Math.toIntExact(Long.valueOf(jsonObject.getString("car")) % 100);
                }

                return idKey.toString();
            }
        }).reduceGroup(new CustomReduceFunction(confJSON)).withBroadcastSet(broadcast, "broadcast").output(new KafkaFormat());




    }
}
