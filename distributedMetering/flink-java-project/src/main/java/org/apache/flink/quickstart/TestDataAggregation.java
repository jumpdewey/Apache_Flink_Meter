package org.apache.flink.quickstart;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Data aggregation class that used by Meter.
 */
public class TestDataAggregation implements
        WindowFunction<DataSourceFormat, Tuple4<Long, Long, Object, Long>, Object, TimeWindow> {
    @Override
    public void apply(
            Object key, TimeWindow timeWindow, Iterable<DataSourceFormat> iterable,
            Collector<Tuple4<Long, Long, Object, Long>> collector) throws Exception {
        long sum = 0;
        for (DataSourceFormat d: iterable) {
            sum += d.getValue();
        }
        collector.collect(new Tuple4<>(timeWindow.getStart(), timeWindow.getEnd(), key, sum));
    }
}
