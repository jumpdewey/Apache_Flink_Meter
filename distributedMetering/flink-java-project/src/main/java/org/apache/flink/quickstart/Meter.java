package org.apache.flink.quickstart;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Meter will support multiple data sources to access data and use transformations to aggregate data
 * in multiple different time windows.
 */
public class Meter {

    public enum Window {
        DAY, HOUR, MINUTE, SECOND
    }
    private static final int CHECKPOINT_TIME = 1000;
    private static final int RESTART_ATTEMPTS = 60;
    private static final int DELAY_INTERVAL = 10;
    private static Logger logger = LoggerFactory.getLogger(Meter.class);

    private final StreamExecutionEnvironment env;

    public Meter() {
        // set up streaming execution environment
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    public void setCheckPoint() {
        this.setCheckPoint(RESTART_ATTEMPTS, DELAY_INTERVAL);
    }

    /**
     * Set checkpoint and restart strategy.
     * @param restart_attempts number of restart attempts
     * @param restart_interval_sec time of interval between two restarts
     */
    public void setCheckPoint(int restart_attempts, int restart_interval_sec) {
        logger.info("Setting checkpoints...");
        logger.info("restart_attempts: "+ RESTART_ATTEMPTS + ", delay_interval: " + DELAY_INTERVAL);
        // set up checkpoint
        String checkpointPath = "file://" + System.getProperty("user.dir") + "/tmp/checkpoints";
        logger.info("Checkpoints file path: " + checkpointPath);
        this.env.setStateBackend(new FsStateBackend(checkpointPath));
        this.env.enableCheckpointing(CHECKPOINT_TIME);
        this.env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                restart_attempts, org.apache.flink.api.common.time.Time.of(restart_interval_sec, TimeUnit.SECONDS)));
    }

    public DataStream<DataSourceFormat> getDataSourceFromFile(String filepath) {
        return this.getDataSourceFromFile(filepath, 1, CheckpointedDataSource.DataType.ECS_EVENT);
    }

    /**
     * Load source data from file with speed_factor.
     * @param filepath file path
     * @param speed_factor please follow the instruction at CheckpointedDataSource class definition.
     * @param type CheckpointedDataSource.DataType to specify what kind of data type is loading.
     * @return data stream of DataSourceFormat
     */
    public DataStream<DataSourceFormat> getDataSourceFromFile(
            String filepath, int speed_factor, CheckpointedDataSource.DataType type) {
        logger.info("Getting data from file: " + filepath);
        logger.info("speed factor is " + speed_factor);
        return env.addSource(new CheckpointedDataSource(filepath, speed_factor, type));
    }

    /**
     * Get aggregation on input data stream with input time window and number.
     * @param data raw data stream
     * @param window Meter.Window that represent
     * @param num number of Meter.Window
     * @return Tuple (window start time, window end time, KEY, sum)
     */
    public DataStream<Tuple4<Long, Long, Object, Long>> aggregateWithMeterWindow(
            DataStream<DataSourceFormat> data, Window window, long num) {
        if (num < 0) {
            logger.error("Input number is invalid!");
            throw new RuntimeException();
        }
        Time time = null;
        switch(window) {
            case DAY:
                time = Time.days(num);
                break;
            case HOUR:
                time = Time.hours(num);
                break;
            case MINUTE:
                time = Time.minutes(num);
                break;
            case SECOND:
                time = Time.seconds(num);
        }
        return data.keyBy(d -> d.getKey()).timeWindow(time).apply(new TestDataAggregation());
    }

    /**
     * Write output to CSV file and execute the whole Flink plan.
     * @param data data stream to output
     * @param outputName output file's name
     */
    public void writeToFile(DataStream data, String outputName) {
        try {
            logger.info("Writing output to " + outputName);
            data.writeAsCsv(outputName, FileSystem.WriteMode.OVERWRITE);
            this.env.execute("Meter Demo!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
