package org.apache.flink.quickstart;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generate EVENT_NUM TestData with ID between ID_MIN and ID_MAX, value between VALUE_MIN and VALUE_MAX.
 *
 * When generating time stamp, there may be some delay up to DELAY_SECOND.
 */
public class TestDataGenerator {
    private static final Calendar CAL = Calendar.getInstance();
    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
    private static final int EVENT_NUM = 1000;
    private static final long TIMESTAMP_BASE = 1500000000;
    private static final int VALUE_MIN = 10;
    private static final int VALUE_MAX = 100;

    private static final int DELAY_SECOND = 3600 * 3; // Assume events' maximum delay time is 3 hours.

    private static String generateDataFileName() {
        return "DataSource_" + FORMAT.format(CAL.getTime()) + ".csv";
    }

    public static String generateOutputFileName() {
        return "DataSink_" + FORMAT.format(CAL.getTime()) + ".csv";
    }

    /**
     * Generate data file to *_DataSource_*.csv
     * @param day to specify the time span of generated data. For example, if input is 3, this method would generate
     *           EVENT_NUM random TestData with value between VALUE_MIN and VALUE_MAX whose timestamp
     *           is between TIMESTAMP_BASE
     *           and time which is 3 days after TIMESTAMP_BASE.
     * @throws IOException
     */
    public static void generateTestData(int day) throws IOException {
        final int ID_MIN = 1;
        final int ID_MAX = 5;
        File file = new File("TestData_" + generateDataFileName());
        file.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));

        int diff = DELAY_SECOND;
        Random rand =new Random();
        int num = 0, tmp_time = 0, tmp_val = 0, tmp_id = 0;
        int gap = day * 86400 / EVENT_NUM;
        while (num++ < EVENT_NUM) {
            tmp_id = rand.nextInt(ID_MAX)+ID_MIN;
            tmp_time = rand.nextInt(diff);
            tmp_val = rand.nextInt(VALUE_MAX - VALUE_MIN +1) + VALUE_MIN;
            // use suffix "000" to make timestamp to milliseconds format, which is required by Flink.
            String tmp_data = TIMESTAMP_BASE + num*gap + tmp_time + "000," + tmp_val + ","
                    + tmp_id + System.lineSeparator();
            bw.write(tmp_data);
        }

        bw.close();
    }

    /**
     * Generate ECS event data file to *_DataSource_*.csv
     * @param day to specify the time span of generated data. For example, if input is 3, this method would generate
     *           EVENT_NUM random ECSEvent whose timestamp is between TIMESTAMP_BASE and time which is 3 days
     *           after TIMESTAMP_BASE.
     * @throws IOException
     */
    public static void generateECSEvent(int day) throws IOException {
        final String[] namespace = {"ns1", "ns2", "ns3"};
        final String[] buckets = {"b1", "b2", "b3", "b4"};
        final long EDITED_BYTES_MIN = -1000000;
        final long EDITED_BYTES_MAX = 1000000;
        final long JOURNAL_NUM_MAX = 100000;
        final long JOURNAL_NUM_MIN = 1;
        File file = new File("ECSEvent_" + generateDataFileName());
        file.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));

        int diff = DELAY_SECOND;
        Random rand =new Random();
        int num = 0, tmp_time, tmp_ns, tmp_bucket;
        long tmp_bytes, tmp_major_journal, tmp_minor_journal;
        int gap = day * 86400 / EVENT_NUM;
        while (num++ < EVENT_NUM) {
            tmp_time = rand.nextInt(diff);
            tmp_ns = rand.nextInt(namespace.length);
            tmp_bucket = rand.nextInt(buckets.length);
            tmp_major_journal = ThreadLocalRandom.current().nextLong((JOURNAL_NUM_MAX - JOURNAL_NUM_MIN) + 1)
                    + JOURNAL_NUM_MIN;
            tmp_minor_journal = ThreadLocalRandom.current().nextLong((JOURNAL_NUM_MAX - JOURNAL_NUM_MIN) + 1)
                    + JOURNAL_NUM_MIN;
            tmp_bytes = ThreadLocalRandom.current().nextLong((EDITED_BYTES_MAX - EDITED_BYTES_MIN) + 1)
                    + EDITED_BYTES_MIN;
            // use suffix "000" to make timestamp to milliseconds format, which is required by Flink.
            String tmp_data = TIMESTAMP_BASE + num*gap + tmp_time + "000," + namespace[tmp_ns] + ","
                    + buckets[tmp_bucket] + "," + tmp_major_journal + "," + tmp_minor_journal + ","
                    + tmp_bytes + System.lineSeparator();
            bw.write(tmp_data);
        }

        bw.close();
    }

    public static void main(String[] args) {
        try {
            //TestDataGenerator.generateTestData(3);
            TestDataGenerator.generateECSEvent(60);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
