package cn.edu.BatchInput.getInputBroadcast;

import com.alibaba.fastjson.JSONObject;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetTaskVar {

    public static List<Map<String, String>> getTaskVarByJson(JSONObject inputDataset) throws SQLException {
        String driverName=null;
        String dbURL=null;
        String userName = inputDataset.getString("DataSourceUser");
        String userPwd = inputDataset.getString("DataSourcePassword");
        String query = inputDataset.getString("Query");
        String dataSourceType = inputDataset.getString("DataSourceType");
        if(dataSourceType.equals("POSTGRESQL")){
            driverName = "org.postgresql.Driver";
            dbURL = "jdbc:postgresql://" + inputDataset.getString("DataSourceIp") + "/" +
                    inputDataset.getString("DataBaseName");
        }
        else if(dataSourceType.equals("SQLSERVER")){
            driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            dbURL = "jdbc:sqlserver://" + inputDataset.getString("DataSourceIp") + ";DatabaseName=" +
                    inputDataset.getString("DataBaseName");
        }
        else if(dataSourceType.equals("MYSQL")){
            driverName = "com.mysql.jdbc.Driver";
            dbURL = "jdbc:mysql://" + inputDataset.getString("DataSourceIp") + "/" +
                    inputDataset.getString("DataBaseName");
        }
        return getTaskVar(driverName, dbURL, userName, userPwd, query);
    }

    public static List<Map<String, String>> getTaskVar(String driverName,String dbURL,String userName,String userPwd,String query) throws SQLException {
        Connection dbConn=null;
        try {
            Class.forName(driverName);
            System.out.println("加载驱动成功！");
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("加载驱动失败！");
        }
        try{
            dbConn=DriverManager.getConnection(dbURL,userName,userPwd);
            System.out.println("连接数据库成功！");
        }catch(Exception e) {
            e.printStackTrace();
            System.out.print("数据库连接失败！");
        }
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = dbConn.createStatement();
            rs = stmt.executeQuery(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        List<Map<String, String>> resList = new ArrayList<Map<String, String>>();
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();
        while(true){
            try {
                if (!rs.next()) break;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            //将获取的数据存入vclinfos中
            Map<String, String> rowData = new HashMap<String, String>();
            for (int i = 1; i <= columnCount; i++) {
                rowData.put(md.getColumnName(i), rs.getString(i));
            }
            resList.add(rowData);
        }
        if(rs != null){   // 关闭记录集
            try{ rs.close() ; }catch(SQLException e){ e.printStackTrace() ; }
        }
        if(stmt != null){   // 关闭声明
            try{ stmt.close() ; }catch(SQLException e){ e.printStackTrace() ; }
        }
        if(dbConn != null){  // 关闭连接对象
            try{ dbConn.close() ; }catch(SQLException e){ e.printStackTrace() ; }
        }
        return resList;
    }

}
