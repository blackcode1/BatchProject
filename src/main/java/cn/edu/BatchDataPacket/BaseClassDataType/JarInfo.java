package cn.edu.BatchDataPacket.BaseClassDataType;

import com.alibaba.fastjson.JSONObject;

public class JarInfo {
    public String jarID;
    public String jarName;
    public String jarPath;
    public String jarClass;
    public String inputType;
    public String outputType;
    public String type;
    public Long windowSize;

    public JarInfo(JSONObject jsonObject) throws Exception{
        this.jarID = jsonObject.getString("JarID");
        this.jarName = jsonObject.getString("JarName");
        this.jarPath = jsonObject.getString("JarPath");
        this.jarClass = jsonObject.getString("JarClass");
        this.inputType = jsonObject.getString("InputType");
        this.outputType = jsonObject.getString("OutputType");
        this.type = jsonObject.getString("Type");
        this.windowSize = jsonObject.getLong("WindowSize");
    }

    @Override
    public String toString() {
        return "JarInfo{" +
                "jarID='" + jarID + '\'' +
                ", jarName='" + jarName + '\'' +
                ", jarPath='" + jarPath + '\'' +
                ", jarClass='" + jarClass + '\'' +
                ", inputType='" + inputType + '\'' +
                ", outputType='" + outputType + '\'' +
                '}';
    }
}
