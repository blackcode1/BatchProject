package thss.rcsdk;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import BatchDataPacket.BaseClassDataType.TaskState;
import cn.edu.thss.rcinterface.RealTimeInterface;
import com.alibaba.fastjson.JSONObject;
import ty.pub.RawDataPacket;
import ty.pub.TransPacket;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

public abstract class RealTimeAlg<T> implements RealTimeInterface<T> {
    private FileOutputStream _out;
    private File _file;
    private int _logLevel;
    public Boolean ready;
    static final int FORBIDDEN = 0;
    static final int SUCCESS = 1;
    static final int FAILURE = 2;

    public RealTimeAlg() {
        this._logLevel = 3;
        this._file = null;
        this._out = null;
    }

    public RealTimeAlg(String logLevel, String logPath) {
        if (logLevel != null || logPath != null) {
            if (logLevel.equals("Null")) {
                this._logLevel = 3;
            } else if (logLevel.equals("Error")) {
                this._logLevel = 2;
            } else if (logLevel.equals("Warning")) {
                this._logLevel = 1;
            } else if (logLevel.equals("Info")) {
                this._logLevel = 0;
            }

            try {
                this._file = new File(logPath);
                this._out = new FileOutputStream(this._file);
                this.ready = true;
            } catch (Exception var4) {
                this.ready = false;
            }
        }

    }

    public RealTimeAlg(String logLevel, String logPath, Boolean callInit, RawDataPacket rawInput, TransPacket transInput, JSONObject jsonInput, List<String> condition, Map<String, List<Map<String, String>>> config, List<TaskState> publicState, TaskState privateState) {
        if (logLevel != null || logPath != null) {
            if (logLevel.equals("Null")) {
                this._logLevel = 3;
            } else if (logLevel.equals("Error")) {
                this._logLevel = 2;
            } else if (logLevel.equals("Warning")) {
                this._logLevel = 1;
            } else if (logLevel.equals("Info")) {
                this._logLevel = 0;
            }

            try {
                this._file = new File(logPath);
                this._out = new FileOutputStream(this._file);
                this.ready = true;
            } catch (Exception var15) {
                this.ready = false;
            }
        }

        if (callInit) {
            try {
                this.init(rawInput, transInput, jsonInput, condition, config, publicState, privateState);
            } catch (Exception var14) {
                var14.printStackTrace();
                String erroutput = var14.toString();
                StackTraceElement ste = var14.getStackTrace()[0];
                erroutput = erroutput + "@Line" + ste.getLineNumber();
                this.toLog(2, erroutput);
            }
        }

    }

    public List<T> callAlg(RawDataPacket rawInput, TransPacket transInput, JSONObject jsonInput, List<String> condition, Map<String, List<Map<String, String>>> config, List<TaskState> publicState, TaskState privateState) {
        try {
            List<T> result = this.calc(rawInput, transInput, jsonInput, condition, config, publicState, privateState);
            if (this._out != null) {
                this._out.close();
            }

            return result;
        } catch (Exception var11) {
            var11.printStackTrace();
            String erroutput = var11.toString();
            StackTraceElement ste = var11.getStackTrace()[0];
            erroutput = erroutput + "@Line" + ste.getLineNumber();
            this.toLog(2, erroutput);
            return null;
        }
    }

    public abstract Boolean init(RawDataPacket var1, TransPacket var2, JSONObject var3, List<String> var4, Map<String, List<Map<String, String>>> var5, List<TaskState> var6, TaskState var7) throws Exception;

    protected abstract List<T> calc(RawDataPacket var1, TransPacket var2, JSONObject var3, List<String> var4, Map<String, List<Map<String, String>>> var5, List<TaskState> var6, TaskState var7) throws Exception;

    protected int toLog(int level, String msg) {
        if (this._out == null) {
            return 0;
        } else {
            try {
                if (!this._out.getFD().valid()) {
                    return 0;
                }
            } catch (Exception var5) {
                return 0;
            }

            if (level >= this._logLevel) {
                try {
                    this._out.write(msg.getBytes());
                    return 1;
                } catch (Exception var4) {
                    return 2;
                }
            } else {
                return 0;
            }
        }
    }
}
