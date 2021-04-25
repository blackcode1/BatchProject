package BatchDataPacket.DataTypeChange;

import BatchDataPacket.DataType;
import BatchDataPacket.SubClassDataType.ParsedDataPacketList;
import edu.thss.entity.ParsedDataPacket;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;


public class MapDatatype2PDP implements FlatMapFunction<DataType, ParsedDataPacket> {

    @Override
    public void flatMap(DataType dataType, Collector<ParsedDataPacket> collector) throws Exception {
        if(dataType.streamDataType.equals("ParsedDataPacket")){
            ParsedDataPacketList transListRes = (ParsedDataPacketList) dataType;
            ParsedDataPacket resultTrans = transListRes.streamData;
            collector.collect(resultTrans);
        }
    }
}
