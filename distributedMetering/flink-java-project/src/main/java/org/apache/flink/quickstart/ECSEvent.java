package org.apache.flink.quickstart;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * ECSEvent is supposed to be the data format from ECS.
 */
public class ECSEvent implements DataSourceFormat{
    private final long timestamp;
    private final String namespace;
    private final String bucket;
    private final long major_journal;
    private final long minor_journal;
    private final long edited_bytes;
    private static final int FIELD_NUM = 6;

    private ECSEvent(long timestamp,
                     String namespace, String bucket, long major_journal, long minor_journal, long edited_bytes) {
        this.timestamp = timestamp;
        this.namespace = namespace;
        this.bucket = bucket;
        this.major_journal = major_journal;
        this.minor_journal = minor_journal;
        this.edited_bytes = edited_bytes;
    }

    public static DataSourceFormat fromString(String line) {
        String[] tokens = line.split(",");
        if (tokens.length != FIELD_NUM) {
            throw new RuntimeException("Invalid Record: " + line);
        }
        return new ECSEvent(Long.parseLong(tokens[0]), tokens[1], tokens[2],
                Long.parseLong(tokens[3]), Long.parseLong(tokens[4]), Long.parseLong(tokens[5]));
    }

    @Override
    public long getEventTime() {
        return this.timestamp;
    }

    @Override
    public <T extends Tuple> T getKey() {
        return (T) new Tuple2(this.namespace, this.bucket);
    }

    @Override
    public long getValue() {
        return this.edited_bytes;
    }

    @Override
    public String toString() {
        return "ECSEvent{" +
                "timestamp=" + timestamp +
                ", namespace='" + namespace + '\'' +
                ", bucket='" + bucket + '\'' +
                ", major_journal=" + major_journal +
                ", minor_journal=" + minor_journal +
                ", edited_bytes=" + edited_bytes +
                '}';
    }
}
