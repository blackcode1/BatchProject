package BatchInput.getInputBatch;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;


public class KafkaInput implements InputFormat {


    public void configure(Configuration configuration) {

    }

    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    public InputSplit[] createInputSplits(int i) throws IOException {
        return new InputSplit[0];
    }

    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return null;
    }

    public void open(InputSplit inputSplit) throws IOException {

    }

    public boolean reachedEnd() throws IOException {
        return false;
    }

    public Object nextRecord(Object o) throws IOException {
        return null;
    }

    public void close() throws IOException {

    }
}
