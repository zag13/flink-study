package com.github.zag13.stock.source;

import com.github.zag13.stock.model.StockPrice;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StockSource implements SourceFunction<StockPrice> {

    private boolean isRunning = true;
    private String path;
    private InputStream streamSource;

    public StockSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<StockPrice> sourceContext) throws Exception {
        // 创建一个BufferedReader对象，读取数据集文件
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(streamSource));

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss");
        String line;
        boolean isFirstLine = true;
        long timeDiff = 0;
        long lastEventTs = 0;

        while (isRunning && (line = br.readLine()) != null) {
            String[] itemStrArr = line.split(",");
            LocalDateTime dateTime = LocalDateTime.parse(itemStrArr[1] + " " + itemStrArr[2], formatter);
            long eventTs = Timestamp.valueOf(dateTime).getTime();
            if (isFirstLine) {
                lastEventTs = eventTs;
                isFirstLine = false;
            }

            StockPrice stock = StockPrice.of(itemStrArr[0], Double.parseDouble(itemStrArr[3]), eventTs, Integer.parseInt(itemStrArr[4]));

            timeDiff = eventTs - lastEventTs;
            if (timeDiff > 0)
                Thread.sleep(timeDiff);

            sourceContext.collect(stock);
            lastEventTs = eventTs;
        }
    }

    // 停止发送数据
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
