package BatchInput.getInputBatch;

import com.google.gson.JsonObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class KafkaSource extends RichSourceFunction<JsonObject> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void run(SourceContext<JsonObject>  sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
