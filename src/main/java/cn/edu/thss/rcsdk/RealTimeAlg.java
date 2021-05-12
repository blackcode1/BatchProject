package cn.edu.thss.rcsdk;
import StreamDataPacket.BaseClassDataType.TaskState;
import cn.edu.thss.rcinterface.RealTimeInterface;
import com.alibaba.fastjson.JSONObject;
import ty.pub.RawDataPacket;
import ty.pub.TransPacket;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class RealTimeAlg<T> implements RealTimeInterface<T> {

    private FileOutputStream _out;
    private File _file;
    private int _logLevel;
    public Boolean ready;
    final static int FORBIDDEN = 0;
    final static int SUCCESS = 1;
    final static int FAILURE = 2;

    public RealTimeAlg() {
        _logLevel = 3;
        _file = null;
        _out = null;
    }

    public RealTimeAlg(BatchAlg<T> outlineAlg, Long timeWindow) {
        _logLevel = 3;
        _file = null;
        _out = null;
    }

    public RealTimeAlg(String logLevel, String logPath) {
        if ((logLevel != null) || (logPath != null)) {
            if (logLevel.equals("Null")) {
                _logLevel = 3;
            } else if (logLevel.equals("Error")) {
                _logLevel = 2;
            } else if (logLevel.equals("Warning")) {
                _logLevel = 1;
            } else if (logLevel.equals("Info")) {
                _logLevel = 0;
            }
            try {
                _file = new File(logPath);
                _out = new FileOutputStream(_file);
                ready = true;
            } catch (Exception e) {
                ready = false;
            }
        }
    }

    public RealTimeAlg(String logLevel, String logPath, Boolean callInit,
                RawDataPacket rawInput, TransPacket transInput, JSONObject jsonInput,
                List<String> condition, Map<String, List<Map<String, String>>> config,
                List<TaskState> publicState, TaskState privateState) {
        if ((logLevel != null) || (logPath != null)) {
            if (logLevel.equals("Null")) {
                _logLevel = 3;
            } else if (logLevel.equals("Error")) {
                _logLevel = 2;
            } else if (logLevel.equals("Warning")) {
                _logLevel = 1;
            } else if (logLevel.equals("Info")) {
                _logLevel = 0;
            }
            try {
                _file = new File(logPath);
                _out = new FileOutputStream(_file);
                ready = true;
            } catch (Exception e) {
                ready = false;
            }
        }
        if (callInit) {
            try {
                init(rawInput, transInput, jsonInput, condition, config, publicState, privateState);
            } catch (Exception e) {
                e.printStackTrace();
                String erroutput = e.toString();
                StackTraceElement ste = e.getStackTrace()[0];
                erroutput = erroutput + "@Line" + ste.getLineNumber();
                toLog(2, erroutput);
            }
        }
    }

    public List<T> callAlg(RawDataPacket rawInput,  TransPacket transInput, JSONObject jsonInput,
                     List<String> condition, Map<String, List<Map<String, String>>> config,
                     List<TaskState> publicState, TaskState privateState) throws Exception{

         List<T> result = calc(rawInput, transInput, jsonInput, condition, config, publicState, privateState);
         return result;

    }

    public abstract Boolean init(RawDataPacket rawInput,  TransPacket transInput, JSONObject jsonInput,
                                 List<String> condition, Map<String, List<Map<String, String>>> config,
                                 List<TaskState> publicState, TaskState privateState) throws Exception;

    protected abstract List<T> calc(RawDataPacket rawInput, TransPacket transInput, JSONObject jsonInput,
                     List<String> condition, Map<String, List<Map<String, String>>> config,
                     List<TaskState> publicState, TaskState privateState) throws Exception;

    protected int toLog(int level, String msg) {
        if (_out == null) return FORBIDDEN;
        try { if (!_out.getFD().valid()) return FORBIDDEN; }
        catch (Exception e) {
            return FORBIDDEN;
        }
        if (level >= _logLevel) {
            try {
                _out.write(msg.getBytes());
                return SUCCESS;
            } catch (Exception e) {
                return FAILURE;
            }
        } else return FORBIDDEN;
    }
}
