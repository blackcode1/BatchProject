package cn.edu.BatchSink;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.iotdb.session.Session;

import java.io.IOException;

public class HbaseFormat extends RichOutputFormat {

    String ip;
    String port;
    String user;
    String password;
    Session session;
    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {

    }

    @Override
    public void writeRecord(Object o) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
