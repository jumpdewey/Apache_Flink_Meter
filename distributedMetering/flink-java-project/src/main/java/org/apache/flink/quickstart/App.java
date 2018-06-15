package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entrance of Meter, which contains three public meter methods to test ECSEvent:
 * testDataPerDayInLast60Days() measure the last 60 days' data with one day window.
 * testTotalDataPer60Day() measure total bytes edited each bucket for a period of 60 days.
 * testDataPerHourInLast24Hours() measure the last 24 hours' data with one hour window.
 */
public class App {
    private static final int SPEED = 1000;
    private static final String INPUT_FILENAME = System.getProperty("user.dir")
            + "/src/main/resources/ECSEvent_DataSource_2018-06-13_12:02:56.csv";
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static Meter m;
    // For real data stream event, this will be replaced to (current_timestamp - 24hour),
    // 1505101612000l is only used for ECSEvent_DataSource_2018-06-13_12:02:56.csv.
    private static final long TIMESTAMP_24_HOURS_AGO = 1505101612000l;
    // 1500004012000l is only used for ECSEvent_DataSource_2018-06-13_12:02:56.csv.
    private static final long TIMESTAMP_60_DAYS_AGO = 1500004012000l;

    public static void prepareForTest() {
        logger.info("Start App...");
        m = new Meter();
        m.setCheckPoint();
    }

    private static String outputFileName(String testName) {
        return System.getProperty("user.dir") + "/src/main/output/"
                + testName + "_" + TestDataGenerator.generateOutputFileName();
    }

    public static void testDataPerDayInLast60Days() {
        logger.info("*****testDataPerDayInLast60Days*****");
        DataStream<DataSourceFormat> data =
                m.getDataSourceFromFile(INPUT_FILENAME, SPEED, CheckpointedDataSource.DataType.ECS_EVENT);

        DataStream datastream = m.aggregateWithMeterWindow(
                data.filter(d -> d.getEventTime() >= TIMESTAMP_60_DAYS_AGO),Meter.Window.DAY, 1);
        m.writeToFile(datastream, outputFileName("DataPerDayInLast60Days"));
    }

    public static void testTotalDataPer60Day() {
        logger.info("*****testTotalDataPer60Day*****");
        DataStream<DataSourceFormat> data =
                m.getDataSourceFromFile(INPUT_FILENAME, SPEED, CheckpointedDataSource.DataType.ECS_EVENT);

        DataStream datastream = m.aggregateWithMeterWindow(data,Meter.Window.DAY, 60);
        m.writeToFile(datastream, outputFileName("TotalDataPer60Day"));
    }

    public static void testDataPerHourInLast24Hours() {
        logger.info("*****testDataPerHourInLast24Hours*****");
        DataStream<DataSourceFormat> data =
                m.getDataSourceFromFile(INPUT_FILENAME, SPEED, CheckpointedDataSource.DataType.ECS_EVENT);
        DataStream datastream = m.aggregateWithMeterWindow(
                data.filter(d -> d.getEventTime() >= TIMESTAMP_24_HOURS_AGO),Meter.Window.HOUR, 1);
        m.writeToFile(datastream, outputFileName("DataPerHourInLast24Hours"));
    }

    public static void main(String[] args) {
        prepareForTest();
        testDataPerHourInLast24Hours();
        testDataPerDayInLast60Days();
        testTotalDataPer60Day();
    }
}