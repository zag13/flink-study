package com.github.zag13.util.event.source;

import com.github.zag13.util.event.model.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FileSource implements SourceFunction<Event> {

    private final String path;

    private InputStream streamSource;

    private boolean isRunning = true;

    public FileSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
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
            Event event = Event.of();

            switch (itemStrArr[1]) {
                case "visit":
                case "event_update":
                    if (eventLength == 11) {
                        event = Event.of11(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9], itemStrArr[10]);
                    }
                    break;
                case "click":
                case "jump":
                    if (eventLength == 9) {
                        event = Event.of9(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8]);
                    }
                    break;
                case "download":
                    if (eventLength == 12) {
                        event = Event.of12(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9], itemStrArr[10], itemStrArr[11]);
                    }
                    break;
                case "start":
                    if (eventLength == 19) {
                        event = Event.of19(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9], itemStrArr[10], itemStrArr[11], itemStrArr[12], itemStrArr[13], itemStrArr[14], itemStrArr[15], itemStrArr[16], itemStrArr[17], itemStrArr[18]);
                    } else if (eventLength == 20) {
                        event = Event.of20(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9], itemStrArr[10], itemStrArr[11], itemStrArr[12], itemStrArr[13], itemStrArr[14], itemStrArr[15], itemStrArr[16], itemStrArr[17], itemStrArr[18], itemStrArr[19]);
                    } else if (eventLength == 21) {
                        event = Event.of21(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9], itemStrArr[10], itemStrArr[11], itemStrArr[12], itemStrArr[13], itemStrArr[14], itemStrArr[15], itemStrArr[16], itemStrArr[17], itemStrArr[18], itemStrArr[19], itemStrArr[20]);
                    } else if (eventLength == 22) {
                        event = Event.of22(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9], itemStrArr[10], itemStrArr[11], itemStrArr[12], itemStrArr[13], itemStrArr[14], itemStrArr[15], itemStrArr[16], itemStrArr[17], itemStrArr[18], itemStrArr[19], itemStrArr[20], itemStrArr[21]);
                    }
                    break;
                case "register":
                    if (eventLength == 10) {
                        event = Event.of10(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9]);
                    } else if (eventLength == 11) {
                        event = Event.of11(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9], itemStrArr[10]);
                    } else if (eventLength == 12) {
                        event = Event.of12(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9], itemStrArr[10], itemStrArr[11]);
                    }
                    break;
                case "login":
                    if (eventLength == 9) {
                        event = Event.of9(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8]);
                    } else if (eventLength == 10) {
                        event = Event.of10(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9]);
                    } else if (eventLength == 11) {
                        event = Event.of11(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9], itemStrArr[10]);
                    }
                    break;
                case "alive":
                    if (eventLength == 13) {
                        event = Event.of13(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9], itemStrArr[10], itemStrArr[11], itemStrArr[12]);
                    }
                    break;
                case "pay":
                    if (eventLength == 15) {
                        event = Event.of15(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6], itemStrArr[7], itemStrArr[8], itemStrArr[9], itemStrArr[10], itemStrArr[11], itemStrArr[12], itemStrArr[13], itemStrArr[14]);
                    }
                    break;
                case "event_enter":
                    if (eventLength == 7) {
                        event = Event.of7(eventTime, itemStrArr[1], itemStrArr[2], itemStrArr[3], itemStrArr[4], itemStrArr[5], itemStrArr[6]);
                    }
                    break;
            }

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
