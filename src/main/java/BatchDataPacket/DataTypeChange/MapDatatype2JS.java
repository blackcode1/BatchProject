package BatchDataPacket.DataTypeChange;

import BatchDataPacket.DataType;
import BatchDataPacket.SubClassDataType.JsonList;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class MapDatatype2JS implements FlatMapFunction<DataType, String> {

    @Override
    public void flatMap(DataType dataType, Collector<String> collector) throws Exception {
        if(dataType.streamDataType.equals("JSONObject")){
            JsonList jsonListRes = (JsonList)dataType;
            String resultStr =jsonListRes.streamData.toJSONString();
            collector.collect(resultStr);
        }
    }
}
