package BatchDataPacket.BaseClass;

import StreamDataPacket.BaseClassDataType.StreamDataset;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OCProject {
    public String projectID;
    public String projectName;
    public String projectType;
    public Boolean isCheckpoint;
    public Integer checkpointTime;//分钟

    public Integer parallelism;
    public String inputDataType;
    public List<DataSet<JSON>> inputDataSetList;
    public StreamDataset outputDataSet;
    public Map<String, String> context;

    public OCProject(JSONObject jsonObject) throws Exception{
        this.projectID = jsonObject.getString("StreamProjectID");
        this.projectName = jsonObject.getString("StreamProjectName");
        this.projectType = jsonObject.getString("projectType");
        this.isCheckpoint = jsonObject.getBoolean("IsCheckpoint");
        this.checkpointTime = jsonObject.getInteger("CheckpointTime");
        this.parallelism = jsonObject.getInteger("Para");
        this.inputDataType = jsonObject.getString("StreamDataType");
        this.inputDataSetList = new ArrayList<DataSet<JSON>>();
        JSONArray datasetList = jsonObject.getJSONArray("StreamDataSetList");
        for(int i = 0; i < datasetList.size(); i++){
            JSONObject dataset = datasetList.getJSONObject(i);
            DataSet<JSON> dataSet = new DataSet<JSON>(dataset);
            this.inputDataSetList.add(dataSet);
        }
//        this.outputDataSet = new StreamDataset(jsonObject.getJSONObject("OutputDataSet"));
//        this.context = new HashMap<String, String>();
//        for(String key: jsonObject.keySet()){
//            this.context.put(key, jsonObject.getString(key));
//        }
    }

    @Override
    public String toString() {
        return "RCProject{" +
                "projectID='" + projectID + '\'' +
                ", projectName='" + projectName + '\'' +
                ", isCheckpoint=" + isCheckpoint +
                ", checkpointTime=" + checkpointTime +
                ", parallelism=" + parallelism +
                ", inputDataType='" + inputDataType + '\'' +
                ", inputDataSetList=" + inputDataSetList +
                ", outputDataSet=" + outputDataSet +
                ", context=" + context +
                '}';
    }
}
