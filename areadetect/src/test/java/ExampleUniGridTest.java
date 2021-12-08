
import cn.edu.whu.glink.areadetect.core.AreaDetect;
import cn.edu.whu.glink.areadetect.examples.IO.sink.ResultFileSinkFunction;
import cn.edu.whu.glink.areadetect.examples.func.AveGeomKPIGetter;
import cn.edu.whu.glink.areadetect.examples.func.AveLowerJudgeDouble;
import cn.edu.whu.glink.areadetect.examples.grid.UniGridInfoJoiner;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.index.GeographicalGridIndex;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;

import java.time.Duration;

public class ExampleUniGridTest {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ExecutionConfig executionConfig = env.getConfig();
    executionConfig.setLatencyTrackingInterval(50L);
    boolean a = executionConfig.isLatencyTrackingConfigured();
    System.out.println(a);
    // some params
    double partitionSize = 0.1;
    double gridSize = 0.05;
    final GeographicalGridIndex gridIndex = new GeographicalGridIndex(gridSize);
    double distTreshhold = 0.2;

    final SlidingEventTimeWindows windowAssigner = SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(5));
    SingleOutputStreamOperator<DetectUnit> source = env
        .addSource(new ExampleDataSource("/Users/haocheng/IdeaProjects/glink/glink-core/src/test/resources/rasters.txt", gridIndex))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<DetectUnit>forBoundedOutOfOrderness(Duration.ofMinutes(5))
            .withTimestampAssigner((r, timestamp) -> r.getTimestamp()));

    SingleOutputStreamOperator<DetectUnit> abnormalStream = source
        .keyBy(DetectUnit::getId)
        .window(windowAssigner)
        .process(new AveLowerJudgeDouble(1));

    SingleOutputStreamOperator<DetectUnit> richedStream =
        abnormalStream.map(new UniGridInfoJoiner(gridSize, partitionSize, distTreshhold));

    // submit job
    DataStream<Geometry> ds = new AreaDetect(richedStream, windowAssigner,new AveGeomKPIGetter()).process();
    ds.map(r -> ((Tuple) r.getUserData()).getField(3)).print();
    ds.addSink(new ResultFileSinkFunction("result.txt")).setParallelism(1);
    env.execute("abc");
  }
}