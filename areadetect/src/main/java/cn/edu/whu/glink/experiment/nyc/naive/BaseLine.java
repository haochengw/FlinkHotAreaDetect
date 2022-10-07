package cn.edu.whu.glink.experiment.nyc.naive;

import cn.edu.whu.glink.areadetect.examples.grid.UniGridInfoJoiner;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.experiment.nyc.Main;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.LocalDateTime;

public class BaseLine {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);

    double rasterSize = 0.05;
    long detectWindowLen = 15;
    long detectWindowStep = 15;
    int partitionSize = 5;
    double distThreshold = 0.05;
    int thres = 10;

    System.out.println(LocalDateTime.now());
    long start = System.currentTimeMillis();

    KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("nycdata")
        .setGroupId("my-group")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setBounded(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema()).build();

    SingleOutputStreamOperator<Tuple2<DetectUnit, Boolean>> raw = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
        .map(new Main.Parser(rasterSize))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<DetectUnit, Boolean>>forBoundedOutOfOrderness(Duration.ofMinutes(5))
            .withTimestampAssigner((r, timestamp) -> r.f0.getTimestamp()));

    // pick-up and drop-off stream
    SingleOutputStreamOperator<DetectUnit> pickUpStream = raw.filter(r -> r.f1).map(r -> r.f0);
    SingleOutputStreamOperator<DetectUnit> dropOffStream = raw.filter(r -> !r.f1).map(r -> r.f0);

    // define window
    SlidingEventTimeWindows slidingWindow = SlidingEventTimeWindows.of(Time.minutes(detectWindowLen), Time.minutes(detectWindowStep));

    // 热点区域网格识别
    SingleOutputStreamOperator<DetectUnit> hsPickUpStream = pickUpStream.keyBy(DetectUnit::getId)
        .window(slidingWindow)
        .aggregate(new Main.SumCountAggregator())
        .filter(r -> (Integer) r.getVal() > thres);


    // 用户自定义的维表初始化与检测单元信息JOIN
    SingleOutputStreamOperator<DetectUnit> richedStream =
        hsPickUpStream.map(new UniGridInfoJoiner(rasterSize, partitionSize, distThreshold));

    // 热点区域识别
    new BaseLineAreaDetect(richedStream, slidingWindow, new Main.AveHeatVal(), distThreshold).process();
    env.execute("abc");
  }
}
