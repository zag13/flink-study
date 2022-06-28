package com.github.zag13.event.source;

import com.github.zag13.event.model.BaseEvent;
import com.github.zag13.event.model.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FileSource implements SourceFunction<BaseEvent> {

    private final String path;

    private InputStream streamSource;

    private boolean isRunning = true;

    public FileSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<BaseEvent> sourceContext) throws Exception {
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        assert streamSource != null;
        BufferedReader br = new BufferedReader(new InputStreamReader(streamSource));

        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String line;

        while (isRunning && (line = br.readLine()) != null) {
            String[] itemStrArr = line.split("\t");

            LocalDateTime dateTime = LocalDateTime.parse(itemStrArr[0], timeFormatter);
            long eventTime = Timestamp.valueOf(dateTime).getTime();
            int eventLength = itemStrArr.length;
            BaseEvent event = BaseEvent.of(line);



            sourceContext.collect(event);
        }
    }

    @Override
    public void cancel() {
        try {
            streamSource.close();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        isRunning = false;
    }
}
