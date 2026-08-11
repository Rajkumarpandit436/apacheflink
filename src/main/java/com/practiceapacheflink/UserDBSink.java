package com.practiceapacheflink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

public class UserDBSink implements Sink<User> {
    @Override
    public SinkWriter<User> createWriter(InitContext context) {
        return new UserDBSinkWriter();
    }

}
