package cn.edu.BatchInput.getInputBroadcast;

import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;

public class GetTaskInfo {
    public static JSONObject getTaskInfo(String projectID, String engineUrl)throws Exception{
        JSONObject taskInfo = new JSONObject();
        String taskInfoStr = sendGet(engineUrl+"/activatedtasks?id="+projectID);
        taskInfoStr = "{\"TaskList\":[{\"Condition\":\"*\",\"TaskID\":\"fuelconsumptiontask\",\"ProjectID\":\"fuelconsumption\",\"DeviceList\":\"*\",\"PublicStateIDList\":\"\",\"timeout\":\"1000\",\"outputTopic\":\"\",\"StatePeriod\":\"1800\",\"StateType\":\"list_string\",\"Broadcast\":\"False\",\"JarID\":\"fuelconsumed\",\"InputDataSetList\":\"\",\"TaskState\":1,\"inputTopicList\":\"*\"}],\"StateInfoList\":[],\"InputDataSetList\":[],\"JarInfoList\":[{\"Type\":\"stream\",\"JarClass\":\"rtcf.test.fuelconsumed\",\"InputType\":\"JSONObject\",\"WindowSize\":\"\",\"JarID\":\"fuelconsumed\",\"JarName\":\"油耗累加存储算法\",\"JarPath\":\"D:\\3\\fuelconsume.jar\",\"OutputType\":\"Table\"}]}";
        if(taskInfoStr == null){
            return null;
        }
        taskInfo = JSONObject.parseObject(taskInfoStr);
        return taskInfo;
    }

    public static String sendGet(String url) throws Exception{
        String result = "";
        BufferedReader in = null;
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
        if (in != null) {
            in.close();
        }
        return result;
    }

}
