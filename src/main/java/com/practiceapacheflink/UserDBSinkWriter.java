package com.practiceapacheflink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

@Slf4j
public class UserDBSinkWriter implements SinkWriter<User> {

    @Override
    public void write(User element, Context context) throws IOException, InterruptedException {
        log.info("Writing user to database: {} ", element);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {

    }

    @Override
    public void close() throws Exception {

    }
}
