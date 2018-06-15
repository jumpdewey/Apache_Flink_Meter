package org.apache.flink.quickstart;

import org.apache.flink.api.java.tuple.Tuple;

/**
 * DataSourceFormat is the format that fit for the Meter.
 * Any custom data source should implement this DataSourceFormat before using the Meter.
 */
public interface DataSourceFormat  {
    long getEventTime();
    <T extends Tuple> T getKey();
    long getValue();
}
