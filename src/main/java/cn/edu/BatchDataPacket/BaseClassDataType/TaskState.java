package cn.edu.BatchDataPacket.BaseClassDataType;

import java.util.*;

public class TaskState {
    private String stateType;
    private List<String> listState;
    private Map<String, List<String>> mapState;
    private long timeStamp;

    public TaskState() {
        this.stateType = "";
        this.listState = new ArrayList<String>();
        this.mapState = new HashMap<String, List<String>>();
        this.timeStamp = new Date().getTime();
    }

    public TaskState(String stateType) {
        this.stateType = stateType;
        this.listState = new ArrayList<String>();
        this.mapState = new HashMap<String, List<String>>();
        this.timeStamp = new Date().getTime();
    }

    public String getStateType() {
        return stateType;
    }

    public void setStateType(String stateType) {
        this.stateType = stateType;
        this.timeStamp = new Date().getTime();
    }

    public List<String> getListState() throws Exception{
        List<String> stringList = new ArrayList<String>();
        stringList.addAll(this.listState);
        return stringList;
    }

    public Map<String, List<String>> getMapState() throws Exception{
        Map<String, List<String>> listMap = new HashMap<String, List<String>>();
        for(Map.Entry<String, List<String>> entry: this.mapState.entrySet()){
            List<String> list = new ArrayList<String>(entry.getValue());
            listMap.put(entry.getKey(), list);
        }
        return listMap;
    }

    public void setMapState(Map<String, List<String>> mapState)throws Exception {
        this.mapState = mapState;
        this.timeStamp = new Date().getTime();
    }

    public void addMapState(String key, String value)throws Exception{
        if(!this.mapState.containsKey(key)){
            List<String> list = new ArrayList<String>();
            list.add(value);
            this.mapState.put(key, list);
        }
        else {
            this.mapState.get(key).add(value);
        }
        this.timeStamp = new Date().getTime();
    }

    public void updateMapState(String key, List<String> value)throws Exception{
        this.mapState.put(key, value);
        this.timeStamp = new Date().getTime();
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public void addListState(String value)throws Exception{
        this.listState.add(value);
        this.timeStamp = new Date().getTime();
    }

    public void setListState(List<String> list)throws Exception{
        this.listState = new ArrayList<String>();
        this.listState.addAll(list);
        this.timeStamp = new Date().getTime();
    }

    public TaskState clone(){
        TaskState taskState = new TaskState(this.stateType);
        taskState.listState.addAll(this.listState);
        for(Map.Entry<String, List<String>> entry: this.mapState.entrySet()){
            List<String> list = new ArrayList<String>(entry.getValue());
            taskState.mapState.put(entry.getKey(), list);
        }
        return taskState;
    }

    @Override
    public String toString() {
        return "TaskState{" +
                "stateType='" + stateType + '\'' +
                ", listState=" + listState +
                ", mapState=" + mapState +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
