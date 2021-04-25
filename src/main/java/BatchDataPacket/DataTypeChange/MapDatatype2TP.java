package BatchDataPacket.DataTypeChange;

import BatchDataPacket.SubClassDataType.TransPacketList;
import BatchDataPacket.DataType;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import ty.pub.TransPacket;

public class MapDatatype2TP implements FlatMapFunction<DataType, TransPacket> {

    @Override
    public void flatMap(DataType dataType, Collector<TransPacket> collector) throws Exception {
        if(dataType.streamDataType.equals("TransPacket")){
            BatchDataPacket.SubClassDataType.TransPacketList transListRes = (TransPacketList) dataType;
            TransPacket resultTrans = transListRes.streamData;
            collector.collect(resultTrans);
        }
    }
}
