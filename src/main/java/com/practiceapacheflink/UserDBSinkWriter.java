package com.practiceapacheflink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class UserDBSinkWriter implements SinkWriter<User> {
    private static final Logger log = LoggerFactory.getLogger(UserDBSinkWriter.class);

    @Override
    public void write(User element, Context context) throws IOException, InterruptedException {
        log.info("write element={}", element);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {

    }

    @Override
    public void close() throws Exception {

    }
}
