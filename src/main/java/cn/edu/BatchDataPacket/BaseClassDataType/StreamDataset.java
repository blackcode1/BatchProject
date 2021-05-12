package cn.edu.BatchDataPacket.BaseClassDataType;

import com.alibaba.fastjson.JSONObject;
import org.dom4j.Element;

public class StreamDataset {

    public String datasetID;
    public String dataSetTopic;
    public String dataSetGroupID;
    public Integer dataSetOffset;
    public String dataType;
    public String dataSourceID;
    public String dataSourceType;
    public String dataSourceIp;
    public String dataSourcePort;
    public String dataSourceUser;
    public String dataSourcePassword;
    public String rmqExchange;
    public Integer rmqDelay;
    public String dataBase;

    public StreamDataset(JSONObject jsonObject) throws Exception{
        this.datasetID = jsonObject.getString("DataSetID");
        this.dataSetTopic = jsonObject.getString("DataSetTopic");
        this.dataSetGroupID = jsonObject.getString("DataSetGroupID");
        this.dataSetOffset = jsonObject.getInteger("DataSetOffset");
        this.dataType = jsonObject.getString("DataType");
        this.dataSourceID = jsonObject.getString("DataSourceID");
        this.dataSourceType = jsonObject.getString("DataSourceType");
        this.dataSourceIp = jsonObject.getString("DataSourceIp");
        this.dataSourcePort = jsonObject.getString("DataSourcePort");
        this.dataSourceUser = jsonObject.getString("DataSourceUser");
        this.dataSourcePassword = jsonObject.getString("DataSourcePassword");
        this.rmqExchange = jsonObject.getString("RMQExchange");
        this.rmqDelay = jsonObject.getIntValue("RMQDelay");
        this.dataBase = jsonObject.getString("DataBaseName");
    }

    public StreamDataset(Element e)throws Exception {
        this.datasetID = e.element("uuid").getTextTrim();
        this.dataSetTopic = (e.element("topic")==null?null:e.element("topic").getTextTrim());
        this.dataSetGroupID = (e.element("group")==null?null:e.element("group").getTextTrim());
        this.dataSetOffset = (e.element("offset")==null?null:Integer.valueOf(e.element("offset").getTextTrim()));
        this.dataType = (e.element("dataid")==null?null:e.element("dataid").getTextTrim());
        this.rmqExchange = (e.element("rmqexchange")==null?null:e.element("rmqexchange").getTextTrim());
        this.rmqDelay = (e.element("rmqdelay")==null?null: Integer.valueOf(e.element("rmqdelay").getTextTrim()));

        Element e2 = e.element("datasource");
        this.dataSourceID = e2.element("uuid").getTextTrim();
        this.dataSourceType = (e2.element("type")==null?null:e2.element("type").getTextTrim());
        this.dataSourceIp = (e2.element("host")==null?null:e2.element("host").getTextTrim());
        this.dataSourcePort = (e2.element("port")==null?null:e2.element("port").getTextTrim());
        this.dataSourceUser = (e2.element("username")==null?null:e2.element("username").getTextTrim());
        this.dataSourcePassword = (e2.element("password")==null?null:e2.element("password").getTextTrim());
        this.dataBase = (e2.element("table")==null?null:e2.element("table").getTextTrim());
    }

    @Override
    public String toString() {
        return "StreamDataset{" +
                "datasetID='" + datasetID + '\'' +
                ", dataSetTopic='" + dataSetTopic + '\'' +
                ", dataSetGroupID='" + dataSetGroupID + '\'' +
                ", dataSetOffset=" + dataSetOffset +
                ", dataType='" + dataType + '\'' +
                ", dataSourceID='" + dataSourceID + '\'' +
                ", dataSourceType='" + dataSourceType + '\'' +
                ", dataSourceIp='" + dataSourceIp + '\'' +
                ", dataSourcePort='" + dataSourcePort + '\'' +
                ", dataSourceUser='" + dataSourceUser + '\'' +
                ", dataSourcePassword='" + dataSourcePassword + '\'' +
                '}';
    }
}
