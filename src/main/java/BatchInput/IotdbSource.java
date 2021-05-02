package BatchInput;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.sql.*;

public class IotdbSource extends RichSourceFunction<JSONObject> {

    String driver = "org.apache.iotdb.jdbc.IoTDBDriver";
    String url = "jdbc:iotdb://192.168.3.31:6667/";
    String username = "root";
    String password = "root";
    private Connection connection = null;
    private PreparedStatement  statement = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 获取连接
        connection = getConn();
        //执行查询
        statement  = connection.prepareStatement("");
    }

    private Connection getConn() {
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    @Override
    public void run(SourceContext<JSONObject> sourceContext) throws Exception {
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {

            // 将数据发送出去
//            sourceContext.collect();
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        super.close();
        if(connection != null){
            connection.close();
        }
        if (statement != null){
            statement.close();
        }
    }
}
