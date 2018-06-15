package org.apache.flink.quickstart;


import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 *  This SourceFunction generates a data stream of multiple data type(see enum DataType) records which are
 *  read from a .CSV input file. Each record has a timestamp to specify the event time.
 *
 *  In order to simulate a realistic stream source, the SourceFunction serves events with random seconds sleep
 *  and some events may arrive with delay.
 *
 *  The serving speed of the SourceFunction can be adjusted by a serving speed factor.
 *  A factor of 10 increases the logical serving time by a factor of 60, i.e., events of one
 *  minute (60 seconds) are served in 1 second.
 *
 *  In addition this SourceFunction implements the ListCheckpointed interface and can hence able to recover from
 *  failures if the job enables checkpointing.
 */
public class CheckpointedDataSource implements SourceFunction<DataSourceFormat>, ListCheckpointed<Long> {
    // To represent different data types that this source function can generate.
    public enum DataType {
        TEST_DATA, ECS_EVENT
    }

    private final String dataFilePath;
    private final int speed_factor;
    private static final long DELAY = 3600*24*1000; // Assume 1 day delay for watermark
    private final DataType type;
    private transient BufferedReader reader;
    private transient InputStreamReader inputStreamReader;
    private static Logger logger = LoggerFactory.getLogger(CheckpointedDataSource.class);


    // state
    // number of emitted events
    private long eventCnt = 0;

    public CheckpointedDataSource(String dataFilePath) {
        this(dataFilePath, 1, DataType.ECS_EVENT);
    }
    public CheckpointedDataSource(String dataFilePath, int speed_factor, DataType type) {
        this.type = type;
        this.dataFilePath = dataFilePath;
        this.speed_factor = speed_factor;
    }

    /**
     * Record eventCnt for snapshot.
     */
    @Override
    public List<Long> snapshotState(long l, long l1) throws Exception {
        logger.warn("Running snapshot...");
        logger.debug("eventCnt: " + eventCnt);
        return Collections.singletonList(eventCnt);
    }

    /**
     * Restore state from snapshot.
     */
    @Override
    public void restoreState(List<Long> list) throws Exception {
        logger.warn("Running restoreState...");
        for (Long l : list) {
            this.eventCnt = l;
        }
    }

    @Override
    public void run(SourceContext<DataSourceFormat> sourceContext) throws Exception {

        final Object lock = sourceContext.getCheckpointLock();
        inputStreamReader = new InputStreamReader(new FileInputStream(dataFilePath), "UTF-8");
        reader = new BufferedReader(inputStreamReader);

        String line;
        long cnt = 0;
        // skip emitted events to find the checkpoint
        while (cnt < eventCnt && reader.ready() && (line = reader.readLine()) != null) {
            cnt++;
        }
        if (cnt != 0) {
            logger.warn("After skipping emitted events...");
            logger.warn("Meter starts from No." + cnt + " event");
        }
        Random rand = new Random();
        // emit all subsequent events proportional to their timestamp
        while (reader.ready() && (line = reader.readLine()) != null) {
            DataSourceFormat data = null;
            // Generate different data source format based on DataType
            switch (this.type) {
                case ECS_EVENT:
                    data = ECSEvent.fromString(line);
                    break;
                case TEST_DATA:
                    data = TestData.fromString(line);
            }
            long eventTime = data.getEventTime();
            logger.info("***Get event: " + data + " ***");
            synchronized (lock) {
                eventCnt++;
                sourceContext.collectWithTimestamp(data, eventTime);
                // set maximum delay to 1 day.
                sourceContext.emitWatermark(new Watermark(eventTime - DELAY));
            }
            // let thread sleep to simulate event stream.
            int delay_sec = rand.nextInt(60)+1;
            Thread.sleep(delay_sec*1000/speed_factor);
            logger.info("***" + delay_sec + " seconds later***");
        }
        this.reader.close();
        this.reader = null;
        this.inputStreamReader.close();
        this.inputStreamReader = null;
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.inputStreamReader != null) {
                this.inputStreamReader.close();
            }
        } catch(IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.inputStreamReader = null;
        }
    }
}
