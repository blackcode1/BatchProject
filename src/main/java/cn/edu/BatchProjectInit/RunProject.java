package cn.edu.BatchProjectInit;

import StreamDataPacket.SubClassDataType.JsonList;
import cn.edu.BatchDataPacket.BaseClass.DataSet;
import cn.edu.BatchDataPacket.BaseClass.OCProject;
import cn.edu.BatchSink.IotdbFormat;
import cn.edu.BatchCal.CustomReduceFunction;
import StreamDataPacket.DataType;
import cn.edu.BatchSink.KafkaFormat;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import cn.edu.BatchInput.getInputBroadcast.BroadcastSource;


import java.util.*;
public class RunProject {
    public static String logStr;


    public static void main(String[] args) throws Exception {
//        String[] e = {"{\"StreamProjectID\":\"123\",\n" +
//                "\"StreamProjectName\":\"123\",\n" +
//                "\"IsCheckpoint\":0,\n" +
//                "\"projectType\":\"\",\n" +
//                "\"CheckpointTime\":30,\n" +
//                "\"Para\":2,\n" +
//                "\"StreamDataType\":\"KAFKA\",\n" +
//                "\"StreamDataSetList\":[{\n" +
//                "\"DataSetID\":\"1\",\n" +
//                "\"DataSetTopic\":\"test0504\",\n" +
//                "\"DataSetGroupID\":\"test0504\",\n" +
//                "\"DataSetOffset\":1,\n" +
//                "\"DataType\":\"KAFKA\",\n" +
//                "\"DataSourceID\":\"1\",\n" +
//                "\"DataSourceType\":\"KAFKA\",\n" +
//                "\"DataSourceIp\":\"192.168.3.32\",\n" +
//                "\"DataSourcePort\":\"9092\",\n" +
//                "\"DataSourceUser\":\"\",\n" +
//                "\"DataSourcePassword\":\"\",\n" +
//                "\"dataSourceTable\":\"\",\n" +
//                "\"DataSetStartTime\":0,\n" +
//                "\"DataSetEndTime\":1000,\n" +
//                "\"DataSetQuery\":\"\"\n" +
//                "}\n" +
//                "]\n" +
//                "}"};
        String e[] = new String[]{
                "{\"StreamDataSetList\":[{\"DataSourcePort\":\"9092\",\"DataSetGroupID\":\"fuelconsumption\",\"DataSetType\":\"StreamData\",\"Query\":\"\",\"DataSourcePassword\":\"\",\"EndTime\":\"-1\",\"SourceID\":\"qykafka\",\"StartTime\":\"1619625600000\",\"DataSetID\":\"fuelinput\",\"DataBaseName\":\"\",\"DataSourceIp\":\"192.168.3.32\",\"DataSourceType\":\"KAFKA\",\"DataSourceUser\":\"\",\"DataType\":\"JSONObject\",\"DataSetTopic\":\"fuelinput\",\"DataSourceID\":\"qykafka\"}],\"CheckpointTime\":\"10\",\"Para\":\"1\",\"StreamDataType\":\"JSONObject\",\"OutputDataSet\":{\"DataSourcePort\":\"\",\"DataSetGroupID\":\"fuelconsumption\",\"DataSetType\":\"StreamData\",\"Query\":\"select@FuelConsumption@from@root.QD2000.Q200.Head\",\"DataSourcePassword\":\"root\",\"EndTime\":\"1619712000000\",\"SourceID\":\"qyiotdb\",\"StartTime\":\"1619625600000\",\"DataSetID\":\"fueltables\",\"DataBaseName\":\"\",\"DataSourceIp\":\"192.168.3.31:6667\",\"DataSourceType\":\"IOTDB\",\"DataSourceUser\":\"root\",\"DataType\":\"Table\",\"DataSetTopic\":\"\",\"DataSourceID\":\"qyiotdb\"},\"IsCheckpoint\":false,\"StreamProjectID\":\"fuelconsumption\",\"StreamProjectName\":\"油耗统计项目\"}"
        };
        JSONObject confJSON = new JSONObject();
        OCProject ocProject = GetProjectInfo.getProjectInfo(e);
        ExecutionEnvironment  env = ExecutionEnvironment .getExecutionEnvironment();
        SetStreamEnv.setParaCheckPoint(env, ocProject);

        List<DataSource<JSONObject>> sourceList = new ArrayList<>();
        List<DataType> list = new ArrayList<>();
        DataSource<DataType> source1 = null;
        for(DataSet<JSONObject> dataSet:ocProject.inputDataSetList){
            if (dataSet.dataSourceType.equals("KAFKA")){
                Properties props = new Properties();
                props.setProperty("bootstrap.servers", dataSet.dataSourceIp + ":" + dataSet.dataSourcePort);
                props.put("group.id", "tsx");
                props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

                Map<TopicPartition, Long> map = new HashMap<>();
                List<PartitionInfo> flink_order = consumer.partitionsFor(dataSet.dataSetTopic);
                //从半小时前开始消费
                long fetchDataTime = new Date().getTime() - 1000 * 60 * 60 * 250;//dataSet.DataSetStartTime;
                for (PartitionInfo par : flink_order) {
                    map.put(new TopicPartition(dataSet.dataSetTopic, par.partition()), fetchDataTime);
                }
                Map<TopicPartition, OffsetAndTimestamp> parMap = consumer.offsetsForTimes(map);
                List<TopicPartition> list1 = new ArrayList<>();
                for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : parMap.entrySet()) {
                    TopicPartition key = entry.getKey();
                    OffsetAndTimestamp value = entry.getValue();
                    if(value == null){
                        continue;
                    }
                    long offset = value.offset();
//                    System.out.println("key:"+key.partition());
//                    System.out.println("offset:"+offset);
                    //根据消费里的timestamp确定offset
                    if (value != null) {
                        list1.add(key);
                        consumer.assign(list1);
                        consumer.seek(key, offset);
                    }
                }
                TreeMap<Long, String> map1 = new TreeMap<>();
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(record.value());
                        map1.put(record.timestamp(), record.value());
//                        if(record.timestamp() > 1614951256144L){
//                            //break;
//                        }
                        DataType data = (DataType) new JsonList(JSONObject.parseObject(record.value()));
                        list.add(data);
//                        System.out.println("record" + record.value());
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
        Configuration outputConfig = new Configuration();
//        outputConfig.setString("servers", ocProject.outputDataSet.dataSourceIp + ":" + ocProject.outputDataSet.dataSourcePort);
//        outputConfig.setString("topic", ocProject.outputDataSet.dataSetTopic);
//        outputConfig.setString("msgType", "msgType");

        DataSource<List<DataType>> broadcast = env.fromCollection(Collections.singleton(new BroadcastSource("1FD1307152CB2B4AB1CC013A2753DB19", "http://192.168.3.32:8000").getDE()));

        //broad

//        broadcast.print();
//
//
        source1.groupBy(new KeySelector<DataType, String>() {
            @Override
            public String getKey(DataType dataType) throws Exception {
                JsonList data = (JsonList) dataType;
                try {
                    Integer idKey = data.streamData.getString("deviceID").hashCode() % 100;
                    if(StringUtils.isNumeric(data.streamData.getString("deviceID"))) {
                        idKey = Math.toIntExact(Long.valueOf(data.streamData.getString("deviceID")) % 100);
                    }
                    return idKey.toString();
                }catch (Exception e){
                    e.printStackTrace();
                    return "a";
                }
            }
        }).reduceGroup(new CustomReduceFunction(confJSON)).withBroadcastSet(broadcast, "broadcast").output(new IotdbFormat()).withParameters(outputConfig);

        source1.print();
//        env.execute();

    }
}
