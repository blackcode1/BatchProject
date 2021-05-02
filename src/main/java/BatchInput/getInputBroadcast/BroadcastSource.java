package BatchInput.getInputBroadcast;

import StreamDataPacket.DataType;
import StreamDataPacket.SubClassDataType.TaskInfoPacket;
import StreamDataPacket.SubClassDataType.TaskVarPacket;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;
import java.util.*;

public class BroadcastSource {
    private String projectID;
    private String engineUrl;

    public BroadcastSource(String projectID, String engineUrl) {
        this.projectID = projectID;
        this.engineUrl = engineUrl;
    }

    public DataType collectTaskInfo(JSONObject taskInfo) throws Exception {
        DataType taskInfoData = null;
        taskInfoData = (DataType) new TaskInfoPacket(taskInfo);
        return taskInfoData;
    }

    public List<DataType> collectTaskVar(JSONObject taskInfo) throws SQLException {

        Map<String, List<Map<String, String>>> bigPacket = new HashMap<String, List<Map<String, String>>>();
        Map<String, Map<String, List<Map<String, String>>>> filterSamllPacket = new HashMap<String, Map<String, List<Map<String, String>>>>();
        List<DataType> list = new ArrayList<DataType>();

        JSONArray inputDatasetList = taskInfo.getJSONArray("InputDataSetList");
        for(int i = 0; i < inputDatasetList.size(); i++){
            JSONObject inputDataset = inputDatasetList.getJSONObject(i);

            List<Map<String, String>> onetable = GetTaskVar.getTaskVarByJson(inputDataset);

            if(!inputDataset.containsKey("Filter") || inputDataset.getString("Filter").equals("")){
                bigPacket.put(inputDataset.getString("DataSetID"), onetable);
            }
            else {
                String filter = inputDataset.getString("Filter");
                String datasetID = inputDataset.getString("DataSetID");
                for(int j = 0; j < onetable.size(); j++){
                    String vclID = onetable.get(j).get(filter);
                    if(filterSamllPacket.containsKey(vclID)){
                        Map<String, List<Map<String, String>>> oneVclInfo = filterSamllPacket.get(vclID);
                        if(oneVclInfo.containsKey(datasetID)){
                            oneVclInfo.get(datasetID).add(onetable.get(j));
                        }
                        else {
                            List<Map<String, String>> oneRow = new LinkedList<Map<String, String>>();
                            oneRow.add(onetable.get(j));
                            oneVclInfo.put(datasetID, oneRow);
                        }
                    }
                    else {
                        Map<String, List<Map<String, String>>> oneVclInfo = new HashMap<String, List<Map<String, String>>>();
                        List<Map<String, String>> oneRow = new LinkedList<Map<String, String>>();
                        oneRow.add(onetable.get(j));
                        oneVclInfo.put(datasetID, oneRow);
                        filterSamllPacket.put(vclID, oneVclInfo);
                    }
                }
            }
        }
        if(!bigPacket.isEmpty()){
            DataType taskVarDataBig = (DataType) new TaskVarPacket(bigPacket);
            list.add(taskVarDataBig);
        }
        for(Map.Entry<String, Map<String, List<Map<String, String>>>> entry: filterSamllPacket.entrySet()){
            DataType taskVarDataSmall = (DataType) new TaskVarPacket(entry.getValue(), entry.getKey());
            list.add(taskVarDataSmall);
        }


        return list;
    }

    public List<DataType> run() throws Exception {
        List<DataType> res = new ArrayList<>();

        JSONObject taskInfo = null;
        taskInfo = GetTaskInfo.getTaskInfo(this.projectID, this.engineUrl);

        if(taskInfo != null){
            res.add(this.collectTaskInfo(taskInfo));
            List<DataType> list = this.collectTaskVar(taskInfo);
            for(int i = 0; i < list.size(); i++){
                DataType x = list.get(i);
                res.add(x);
            }
        }
        return res;
    }

    public List<Map<String, List<Map<String, String>>>> getDE() throws Exception {
        List<Map<String, List<Map<String, String>>>> res = new ArrayList<>();

        JSONObject taskInfo = null;
        taskInfo = GetTaskInfo.getTaskInfo(this.projectID, this.engineUrl);

        if(taskInfo != null){
            List<DataType> list = this.collectTaskVar(taskInfo);
            for(int i = 0; i < list.size(); i++){
                DataType x = list.get(i);
                TaskVarPacket packet = (TaskVarPacket) x;
                res.add(packet.taskvar);
            }
        }
        return res;
    }
}
