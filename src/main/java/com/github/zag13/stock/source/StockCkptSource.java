package com.github.zag13.stock.source;

import com.github.zag13.stock.model.StockPrice;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

// 修改Source，在读取数据集时使用一个Offset，保证Source有故障恢复的能力
public class StockCkptSource implements SourceFunction<StockPrice>, CheckpointedFunction {

    private int offset;
    private ListState<Integer> offsetState;

    private boolean isRunning = true;
    private final String path;
    private InputStream streamSource;

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // clear previous state
        offsetState.clear();
        // update the latest offset into offsetState
        offsetState.add(offset);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // initialize offsetState
        ListStateDescriptor<Integer> desc = new ListStateDescriptor<Integer>("offset", Types.INT);
        offsetState = functionInitializationContext.getOperatorStateStore().getListState(desc);

        Iterable<Integer> iter = offsetState.get();
        if (iter == null || !iter.iterator().hasNext()) {
            // if it's the first time to call initializeState()
            // set offset to 0
            offset = 0;
        } else {
            // it's recovered from previous checkpoint
            offset = iter.iterator().next();
        }
    }

    public StockCkptSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<StockPrice> sourceContext) throws Exception {
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        assert streamSource != null;
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

            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(stock);
                offset++;
            }

            lastEventTs = eventTs;
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
