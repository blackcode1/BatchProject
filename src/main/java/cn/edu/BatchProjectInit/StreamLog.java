package cn.edu.BatchProjectInit;

import com.alibaba.fastjson.JSONObject;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StreamLog {

    public static String createLog(String level, String msg, String e, String pid){
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        JSONObject logJson = new JSONObject();
        logJson.put("@timestamp", sdf.format(new Date()));
        logJson.put("level", level);
        logJson.put("department", "soft");
        logJson.put("project", "Flink");
        logJson.put("program", "FlinkProject");
        if(pid != null){
            logJson.put("instance_id", pid);
        }
        logJson.put("msg", msg);
        if(e != null){
            logJson.put("exception", e);
        }
        return logJson.toString();
    }

    public static String createExLog(Exception e, String level, String msg){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        pw.close();
        return createLog(level, msg, sw.toString(), null);
    }

    public static String createLocalLog(Exception e, String level, String msg, String pid){

        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        JSONObject logJson = new JSONObject();
        logJson.put("@timestamp", sdf.format(new Date()));
        logJson.put("level", level);
        logJson.put("department", "soft");
        logJson.put("project", "Flink");
        logJson.put("program", "FlinkProject");
        logJson.put("instance_id", pid);
        logJson.put("msg", msg);
        if(e != null){
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            pw.close();
            logJson.put("exception", sw.toString());
        }

        return logJson.toString();
    }

    public static String getExc(Exception e){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        pw.close();
        return sw.toString();
    }
}

