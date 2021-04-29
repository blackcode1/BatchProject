package BatchProjectInit;


import BatchDataPacket.BaseClass.OCProject;
import StreamProjectInit.StreamLog;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;

import org.apache.flink.api.java.ExecutionEnvironment;

public class SetStreamEnv {

    public static String logStr;

    public static void setParaCheckPoint(ExecutionEnvironment env, OCProject ocProject) throws Exception {
        try {
            Integer para = ocProject.parallelism;
            env.setParallelism(para);

            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));

        } catch (Exception e) {
            logStr = StreamLog.createExLog(e, "ERROR", "项目并行度与检查点设置异常");
        }

    }
}
