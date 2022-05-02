package com.flinkpd;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class ProcessfunctionDemo {

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

        s1.keyBy(Order::getUserId)
                .process(new KeyedProcessFunction<Integer, Order, String>() {

                    private long durationMsec;
                    private transient MapState<Long,Integer> sumer;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        durationMsec  = Time.seconds(5).toMilliseconds();
                        MapStateDescriptor<Long,Integer> desc = new MapStateDescriptor<>("secondlyOrders",Long.class,Integer.class);
                        sumer = getRuntimeContext().getMapState(desc);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        Integer sum = sumer.get(timestamp);
                        out.collect(""+ctx.getCurrentKey()+" , " + sum);
                        sumer.remove(timestamp);
                    }

                    @Override
                    public void processElement(Order order, Context ctx, Collector<String> out) throws Exception {
                        long eventTime = order.getEventTime();
                        TimerService timerService = ctx.timerService();

                        if (eventTime>timerService.currentWatermark()){
                            long endOfWindow = (eventTime - (eventTime%durationMsec)+durationMsec -1);
                            Integer sum = sumer.get(endOfWindow);
                            sum = (sum ==null)?order.getMoney():sum+order.getMoney();
                            sumer.put(endOfWindow,sum);
                            timerService.registerEventTimeTimer(endOfWindow);
                        }

                    }
                })
                .print();

        env.execute();
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
