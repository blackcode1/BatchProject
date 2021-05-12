package cn.edu.thss.rcsdk;

import StreamDataPacket.BaseClassDataType.StreamDataset;
import StreamDataPacket.BaseClassDataType.TaskState;
import StreamDataPacket.BaseClassDataType.TransPacketRely.TransPacketSerializationSchema;
import StreamDataPacket.DataType;
import StreamDataPacket.DataTypeChange.MapDatatype2JS;
import StreamDataPacket.DataTypeChange.MapDatatype2TP;
import StreamDataPacket.SubClassDataType.JsonList;
import StreamDataPacket.SubClassDataType.TransPacketList;
import cn.edu.thss.rcsdk.RealTimeAlg;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import ty.pub.TransPacket;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.*;

public class RunUserFunction {

    private String taskID;
    private String engineUrl;
    private Properties inputProps;
    private Map<String, TaskState> taskStateMap;
    private Map<String, Boolean> userAlgInit;
    private List allResultPacket;
    private StreamDataset inputSource;
    private StreamDataset outputSource;

    public RunUserFunction(String taskID, String engineUrl) {
        this.taskID = taskID;
        this.engineUrl = engineUrl;
        this.taskStateMap = new HashMap<String, TaskState>();
        this.userAlgInit = new HashMap<String, Boolean>();
        this.allResultPacket = new ArrayList();
        JSONObject inSource = new JSONObject();
        JSONObject outSource = new JSONObject();
        String taskInfoStr = sendGet(engineUrl+"/localtestinput?id="+taskID);
        inSource = JSONObject.parseObject(taskInfoStr).getJSONObject("inputSource");
        outSource = JSONObject.parseObject(taskInfoStr).getJSONObject("outputSource");
//        this.inputSource = new StreamDataset(inSource);
//        this.outputSource = new StreamDataset(outSource);
        this.inputProps = new Properties();
        String ip = inputSource.dataSourceIp;
        String port = inputSource.dataSourcePort;
        String group = inputSource.dataSetGroupID;
        putIfNull(inputProps, "bootstrap.servers", ip+":"+port);
        putIfNull(inputProps, "group.id", group);
        putIfNull(inputProps, "enable.auto.commit", "false");
        putIfNull(inputProps, "auto.offset.reset", "latest");
        putIfNull(inputProps, "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        putIfNull(inputProps, "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public String getTaskID() {
        return taskID;
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
    }

    public String getEngineUrl() {
        return engineUrl;
    }

    public void setEngineUrl(String engineUrl) {
        this.engineUrl = engineUrl;
    }

    public Map<String, TaskState> getTaskStateMap() {
        return taskStateMap;
    }

    public void setTaskStateMap(Map<String, TaskState> taskStateMap) {
        this.taskStateMap = taskStateMap;
    }

    public Map<String, Boolean> getUserAlgInit() {
        return userAlgInit;
    }

    public void setUserAlgInit(Map<String, Boolean> userAlgInit) {
        this.userAlgInit = userAlgInit;
    }


    public List getAllResultPacket() {
        return allResultPacket;
    }

    public void setAllResultPacket(List allResultPacket) {
        this.allResultPacket = allResultPacket;
    }

    public Properties getInputProps() {
        return inputProps;
    }

    public void setInputProps(Properties inputProps) {
        this.inputProps = inputProps;
    }

    public StreamDataset getInputSource() {
        return inputSource;
    }

    public void setInputSource(StreamDataset inputSource) {
        this.inputSource = inputSource;
    }

    public StreamDataset getOutputSource() {
        return outputSource;
    }

    public void setOutputSource(StreamDataset outputSource) {
        this.outputSource = outputSource;
    }

    private static String sendGet(String url) {
        String result = "";
        BufferedReader in = null;
        try {
            String urlNameString = url;
            URL realUrl = new URL(urlNameString);
            // 打开和URL之间的连接
            URLConnection connection = realUrl.openConnection();
            // 设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            // 建立实际的连接
            connection.connect();
            // 获取所有响应头字段
            Map<String, List<String>> map = connection.getHeaderFields();
            // 定义 BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送GET请求出现异常！" + e);
            e.printStackTrace();
        }
        // 使用finally块来关闭输入流
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }

    public TaskState getTaskStateByDevice(String deviceID) {
        if(!taskStateMap.containsKey(deviceID)){
            taskStateMap.put(deviceID, new TaskState(null));
        }
        return taskStateMap.get(deviceID);
    }

    public List calOnePacket(cn.edu.thss.rcsdk.RealTimeAlg alg, RTCFInput input, Boolean showconfig) throws Exception {
        if(showconfig){
            System.out.println("输入："+input);
        }
        else {
            System.out.println("输入："+input.toStringWOConfig());
        }
        Boolean initResult = true;
        if(!userAlgInit.containsKey(input.deviceID)|| !userAlgInit.get(input.deviceID)){
            initResult = alg.init(input.rawInput, input.transInput, input.jsonInput, input.condition,
                    input.config, input.publicState, input.privateState);
            userAlgInit.put(input.deviceID, initResult);
        }
        System.out.println("初始化情况："+userAlgInit);
        if(initResult){
            List resultPacket = alg.callAlg(input.rawInput, input.transInput, input.jsonInput, input.condition,
                    input.config, input.publicState, input.privateState);
            System.out.println("计算后状态："+taskStateMap);
            System.out.println("输出："+resultPacket);
            if(resultPacket != null){
                if(resultPacket.size() > 0 && outputSource.dataType.equals("TransPacket")
                        && !resultPacket.get(0).getClass().equals(TransPacket.class)){
                    System.out.println("输出数据类型错误，目标类型Transpacket，实际"+resultPacket.get(0).getClass());
                }
                else if(resultPacket.size() > 0 && outputSource.dataType.equals("JSONObject")
                        && !resultPacket.get(0).getClass().equals(JSONObject.class)){
                    System.out.println("输出数据类型错误，目标类型JSONObject，实际"+resultPacket.get(0).getClass());
                }
                else {
                    allResultPacket.addAll(resultPacket);
                    return resultPacket;
                }
            }
        }
        return new ArrayList();
    }

    private List<DataType> packetResult(List list){
        if(list == null){
            return new ArrayList<DataType>();
        }
        List<DataType> res = new ArrayList<DataType>();
        for(int i = 0; i < list.size(); i++){
            if(list.get(i).getClass().equals(TransPacket.class)){
                DataType resDataType = (DataType) new TransPacketList((TransPacket) list.get(i), taskID);
                res.add(resDataType);
            }
            else if(list.get(i).getClass().equals(JSONObject.class)){
                DataType resDataType = (DataType) (DataType) new JsonList((JSONObject) list.get(i), taskID);
                res.add(resDataType);
            }
        }
        return res;
    }

    public void sendResultPacket(List res) throws Exception {
        List<DataType> dataTypes = packetResult(res);
        if(dataTypes != null && dataTypes.size() > 0){
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<DataType> resultStream = env.fromCollection(dataTypes);
            if(outputSource.dataSourceType.equals("KAFKA")){
                String brokerList = outputSource.dataSourceIp + ":" + outputSource.dataSourcePort;
                String topicId = outputSource.dataSetTopic;
                if(outputSource.dataType.equals("JSONObject")){
                    resultStream
                            .flatMap(new MapDatatype2JS())
                            .addSink(new FlinkKafkaProducer<String>(brokerList, topicId, new SimpleStringSchema()));
                }
                else if(outputSource.dataType.equals("TransPacket")){
                    resultStream
                            .flatMap(new MapDatatype2TP())
                            .addSink(new FlinkKafkaProducer<TransPacket>(brokerList, topicId, new TransPacketSerializationSchema()));
                }
            }
            else if(outputSource.dataSourceType.equals("RMQ")){
                RMQConnectionConfig rmqConfig = new RMQConnectionConfig.Builder()
                        .setHost(outputSource.dataSourceIp)
                        .setPort(Integer.valueOf(outputSource.dataSourcePort))
                        .setUserName("")
                        .setPassword("")
                        .build();
                String queueName = outputSource.dataSetTopic;

                if(outputSource.dataType.equals("JSONObject")){
                    resultStream
                            .flatMap(new MapDatatype2JS())
                            .addSink(new RMQSink<String>(rmqConfig, queueName, new SimpleStringSchema()))
                            .setParallelism(1);
                }
                else if(outputSource.dataType.equals("TransPacket")){
                    resultStream
                            .flatMap(new MapDatatype2TP())
                            .addSink(new RMQSink<TransPacket>(rmqConfig, queueName, new TransPacketSerializationSchema()))
                            .setParallelism(1);
                }
            }
            env.execute();
        }
    }

    public void uploadDataToEngine(){
        JSONObject data = new JSONObject();
        data.put("state", taskStateMap);
        data.put("init", userAlgInit);
        String res = sendGet(engineUrl+"/localtestdataup?id="+taskID+"&data="+URLEncoder.encode(data.toString()));
        System.out.println(res);
    }

    public void downloadDataFromEngine(){
        String res = sendGet(engineUrl+"/localtestdatadown?id="+taskID);
        JSONObject data = JSONObject.parseObject(res);
        JSONObject state = (JSONObject) data.get("state");
        for(Map.Entry<String, Object> entry: state.entrySet()){
            taskStateMap.put(entry.getKey(), JSONObject.parseObject(entry.getValue().toString(), TaskState.class));
        }
        userAlgInit = (Map<String, Boolean>) data.get("init");
    }

    private void putIfNull(Properties props, String key, String value){
        if(!props.containsKey(key)){
            props.put(key, value);
        }
    }

    public void runOnce(cn.edu.thss.rcsdk.RealTimeAlg alg, Boolean showconfig)throws Exception{
        System.out.println("测试开始");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(inputProps);
        consumer.subscribe(Collections.singletonList(inputSource.dataSetTopic));
        try {
            while (true) {//消费者是一个长期运行的程序，通过持续轮询向Kafka请求数据。在其他线程中调用consumer.wakeup()可以退出循环
                //在100ms内等待Kafka的broker返回数据.超市参数指定poll在多久之后可以返回，不管有没有可用的数据都要返回
                ConsumerRecords<String, String> records = consumer.poll(100);
                int packetNum = 0;
                for (ConsumerRecord<String, String> record : records) {
                    RTCFInput input = new RTCFInput(JSONObject.parseObject(record.value()));
                    if(input.taskID.equals(this.taskID)){
                        input.privateState = getTaskStateByDevice(input.deviceID);
                        calOnePacket(alg, input, showconfig);
                        packetNum = packetNum + 1;
                    }
                }
                if(packetNum > 0){
                    break;
                }
            }
            System.out.println("测试完成, 所有结果数据包："+this.allResultPacket);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //退出应用程序前使用close方法关闭消费者，网络连接和socket也会随之关闭，并立即触发一次再均衡
            consumer.close();
        }
    }

    public void runOnePeriod(cn.edu.thss.rcsdk.RealTimeAlg alg, Long second, Boolean sendResultInTime, Boolean showconfig)throws Exception{
        System.out.println("测试开始");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(inputProps);
        consumer.subscribe(Collections.singletonList(inputSource.dataSetTopic));
        try {
            Long startTime = new Date().getTime();
            while (true) {//消费者是一个长期运行的程序，通过持续轮询向Kafka请求数据。在其他线程中调用consumer.wakeup()可以退出循环
                //在100ms内等待Kafka的broker返回数据.超市参数指定poll在多久之后可以返回，不管有没有可用的数据都要返回
                ConsumerRecords<String, String> records = consumer.poll(100);
                List results = new ArrayList();
                for (ConsumerRecord<String, String> record : records) {
                    RTCFInput input = new RTCFInput(JSONObject.parseObject(record.value()));
                    if(input.taskID.equals(this.taskID)){
                        input.privateState = getTaskStateByDevice(input.deviceID);
                        List result = calOnePacket(alg, input, showconfig);
                        if(result != null){
                            results.addAll(result);
                        }
                    }
                }
                if(sendResultInTime){
                    sendResultPacket(results);
                }
                Long currentTime = new Date().getTime();
                if(currentTime - startTime > 1000 * second){
                    break;
                }
            }
            System.out.println("测试完成, 所有结果数据包："+this.allResultPacket);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //退出应用程序前使用close方法关闭消费者，网络连接和socket也会随之关闭，并立即触发一次再均衡
            consumer.close();
        }
    }

    public void runOnce(cn.edu.thss.rcsdk.RealTimeAlg alg)throws Exception{
        System.out.println("测试开始");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(inputProps);
        consumer.subscribe(Collections.singletonList(inputSource.dataSetTopic));
        try {
            while (true) {//消费者是一个长期运行的程序，通过持续轮询向Kafka请求数据。在其他线程中调用consumer.wakeup()可以退出循环
                //在100ms内等待Kafka的broker返回数据.超市参数指定poll在多久之后可以返回，不管有没有可用的数据都要返回
                ConsumerRecords<String, String> records = consumer.poll(100);
                int packetNum = 0;
                for (ConsumerRecord<String, String> record : records) {
                    RTCFInput input = new RTCFInput(JSONObject.parseObject(record.value()));
                    if(input.taskID.equals(this.taskID)){
                        input.privateState = getTaskStateByDevice(input.deviceID);
                        calOnePacket(alg, input, true);
                        packetNum = packetNum + 1;
                    }
                }
                if(packetNum > 0){
                    break;
                }
            }
            System.out.println("测试完成, 所有结果数据包："+this.allResultPacket);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //退出应用程序前使用close方法关闭消费者，网络连接和socket也会随之关闭，并立即触发一次再均衡
            consumer.close();
        }
    }

    public void runOnePeriod(RealTimeAlg alg, Long second, Boolean sendResultInTime)throws Exception{
        System.out.println("测试开始");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(inputProps);
        consumer.subscribe(Collections.singletonList(inputSource.dataSetTopic));
        try {
            Long startTime = new Date().getTime();
            while (true) {//消费者是一个长期运行的程序，通过持续轮询向Kafka请求数据。在其他线程中调用consumer.wakeup()可以退出循环
                //在100ms内等待Kafka的broker返回数据.超市参数指定poll在多久之后可以返回，不管有没有可用的数据都要返回
                ConsumerRecords<String, String> records = consumer.poll(100);
                List results = new ArrayList();
                for (ConsumerRecord<String, String> record : records) {
                    RTCFInput input = new RTCFInput(JSONObject.parseObject(record.value()));
                    if(input.taskID.equals(this.taskID)){
                        input.privateState = getTaskStateByDevice(input.deviceID);
                        List result = calOnePacket(alg, input, true);
                        if(result != null){
                            results.addAll(result);
                        }
                    }
                }
                if(sendResultInTime){
                    sendResultPacket(results);
                }
                Long currentTime = new Date().getTime();
                if(currentTime - startTime > 1000 * second){
                    break;
                }
            }
            System.out.println("测试完成, 所有结果数据包："+this.allResultPacket);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //退出应用程序前使用close方法关闭消费者，网络连接和socket也会随之关闭，并立即触发一次再均衡
            consumer.close();
        }
    }

}
