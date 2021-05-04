package BatchSink;

import StreamDataPacket.DataType;
import StreamDataPacket.SubClassDataType.DBStore.DBCondition;
import StreamDataPacket.SubClassDataType.DBStore.DBSQL;
import StreamDataPacket.SubClassDataType.DBStore.DBStore;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IotdbBatchSink extends RichOutputFormat<DataType> {

    String ip;
    String port;
    String user;
    String password;
    Session session;

    public IotdbBatchSink(String ip, String user, String password) {
        this.ip = ip.split(":")[0];
        this.port = ip.split(":")[1];
        this.user = user;
        this.password = password;
    }
    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) {
        session = new Session(ip, port, user, password);
        try {
            session.open();
        }catch (IoTDBConnectionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeRecord(DataType value) {
        if(value.streamDataType.equals("DBStore")) {
            DBStore dbStore = (DBStore) value;
            if (dbStore.getBatchStore()) {
                List<DBSQL> dbsqls = dbStore.getSqls();
                // RowBatch rowBatch = createRowBatch(dbsqls.get(0), dbsqls.size(), dbStore.getTableName());
                Tablet tablet = createRowBatch(dbsqls.get(0), dbsqls.size(), dbStore.getTableName());
                for (DBSQL dbsql: dbsqls) {
                    setRowBatchValue(tablet, dbsql.getConditons(), dbsql.getTimeStamp());
                }
                if(session == null){
                    session = new Session(ip, port, user, password);
                    try {
                        session.open();
                    } catch (IoTDBConnectionException e) {
                        e.printStackTrace();
                    }

                }
                try {
                    session.insertTablet(tablet);
                } catch (Exception e){
                    session = new Session(ip, port, user, password);
                    try {
                        session.open();
                        session.insertTablet(tablet);
                    } catch (StatementExecutionException ex) {
                        ex.printStackTrace();
                    } catch (IoTDBConnectionException ex) {
                        ex.printStackTrace();
                    }
                }

            }
            else {
                for(DBSQL dbsql: dbStore.getSqls()){
                    Tablet tablet = createRowBatch(dbsql, 1, dbStore.getTableName());
                    setRowBatchValue(tablet, dbsql.getConditons(), dbsql.getTimeStamp());
                    if(session == null){
                        session = new Session(ip, port, user, password);
                        try {
                            session.open();
                        } catch (IoTDBConnectionException ex) {
                            ex.printStackTrace();
                        }
                    }
                    try {
                        session.insertTablet(tablet);
                    } catch (Exception e){
                        session = new Session(ip, port, user, password);
                        try {
                            session.open();
                            session.insertTablet(tablet);
                        } catch (StatementExecutionException ex) {
                            ex.printStackTrace();
                        } catch (IoTDBConnectionException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }

        }
    }

    @Override
    public void close() throws IOException {
        if (session != null) {
            try {
                session.close();
            } catch (IoTDBConnectionException e) {
                e.printStackTrace();
            }

        }
    }

    public Tablet createRowBatch(DBSQL dbsql, int batchSize, String tableName){
        List<DBCondition> dbConditions = dbsql.getConditons();
        List<MeasurementSchema> schemas = new ArrayList<>();
        for (DBCondition condition : dbConditions) {
            if (condition.getDataType().equals("Int")) {
                schemas.add(new MeasurementSchema(condition.getColName(), TSDataType.INT32, TSEncoding.RLE));
            } else if (condition.getDataType().equals("Long")) {
                schemas.add(new MeasurementSchema(condition.getColName(), TSDataType.INT64, TSEncoding.RLE));
            } else if (condition.getDataType().equals("Float")) {
                schemas.add(new MeasurementSchema(condition.getColName(), TSDataType.FLOAT, TSEncoding.GORILLA));
            } else if (condition.getDataType().equals("Double")) {
                schemas.add(new MeasurementSchema(condition.getColName(), TSDataType.DOUBLE, TSEncoding.GORILLA));
            } else if (condition.getDataType().equals("Boolean")) {
                schemas.add(new MeasurementSchema(condition.getColName(), TSDataType.BOOLEAN, TSEncoding.RLE));
            } else {
                schemas.add(new MeasurementSchema(condition.getColName(), TSDataType.TEXT, TSEncoding.PLAIN));
            }
        }
        String table = tableName;
        if (dbsql.getRowKey() != null) {
            table = table + "." + dbsql.getRowKey();
        }
        Tablet tablet = new Tablet(table, schemas, batchSize);
        return tablet;
    }

    public void setRowBatchValue(Tablet tablet, List<DBCondition> dbConditions, Long time){
        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;
        int row = tablet.rowSize++;
        timestamps[row] = time;
        for (int j = 0; j < dbConditions.size(); j++) {
            DBCondition condition = dbConditions.get(j);
            if (condition.getDataType().equals("Int")) {
                int[] sensor = (int[]) values[j];
                sensor[row] = Integer.parseInt(dbConditions.get(j).getValue());
            } else if (condition.getDataType().equals("Long")) {
                long[] sensor = (long[]) values[j];
                sensor[row] = Long.parseLong(dbConditions.get(j).getValue());
            } else if (condition.getDataType().equals("Float")) {
                float[] sensor = (float[]) values[j];
                sensor[row] = Float.parseFloat(dbConditions.get(j).getValue());
            } else if (condition.getDataType().equals("Double")) {
                double[] sensor = (double[]) values[j];
                sensor[row] = Double.parseDouble(dbConditions.get(j).getValue());
            } else if (condition.getDataType().equals("Boolean")) {
                boolean[] sensor = (boolean[]) values[j];
                sensor[row] = Boolean.parseBoolean(dbConditions.get(j).getValue());
            } else {
                Binary[] sensor = (Binary[]) values[j];
                sensor[row] = new Binary(dbConditions.get(j).getValue());
            }
        }
    }
}
