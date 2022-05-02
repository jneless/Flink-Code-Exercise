package com.flinkpd;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


public class ApplyAPI {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Order> s1 = env.addSource(new RichSourceFunction<Order>() {
            @Override
            public void run(SourceContext<Order> ctx) {
                ctx.collect(new Order("A", 1, 100, 1634466573263L)); // 1,33
                ctx.collect(new Order("B", 2, 100, 1634466573728L)); // 2,33
                ctx.collect(new Order("A", 1, 100, 1634466575728L)); // 1.35
                ctx.collect(new Order("C", 2, 100, 1634466576728L)); // 2,36
                ctx.collect(new Order("D", 1, 100, 1634466579728L)); // 1,39
                ctx.collect(new Order("A", 1, 100, 1634466581728L)); // 1,41
                ctx.collect(new Order("A", 1, 100, 1634466574728L)); // 1,34
                ctx.collect(new Order("D", 4, 100, 1634466575728L)); // 4,35
            }

            @Override
            public void cancel() {

            }
        });

        // TODO
        // please implement the logic of ProcessfunctionDemo.java under window ApplyAPI
        // such as : window.apply( new RichWindowFunction<T,W,D,S>(T,W,D,S) )

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;
    }
}
