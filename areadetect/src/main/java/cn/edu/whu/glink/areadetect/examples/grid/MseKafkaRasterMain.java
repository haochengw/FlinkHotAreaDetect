package cn.edu.whu.glink.areadetect.examples.grid;

import cn.edu.whu.glink.areadetect.core.AreaDetect;
import cn.edu.whu.glink.areadetect.examples.IO.sink.ResultFileSinkFunction;
import cn.edu.whu.glink.areadetect.examples.IO.source.MseKafkaDataSource;
import cn.edu.whu.glink.areadetect.examples.IO.source.helper.MseString2GridDetectionUnit;
import cn.edu.whu.glink.areadetect.examples.func.AveGeomKPIGetter;
import cn.edu.whu.glink.areadetect.examples.func.AveLowerJudgeDouble;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.index.GeographicalGridIndex;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;

import java.time.Duration;

public class MseKafkaRasterMain {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    String sinkFile = "/home/hadoop/raster_result.txt";
    double partitionGridSize = 5;
    double distTreshhold = 0.2;
    double rasterSize = 0.05;
    double abnormalThreshold = 0;
    long detectWindowLen = 300;
    long detectWindowStep = 300;

    GeographicalGridIndex rasterIndex = new GeographicalGridIndex(rasterSize);

    SingleOutputStreamOperator<DetectUnit> source =
        MseKafkaDataSource.getSource(env)
            .map(new MseString2GridDetectionUnit(rasterIndex))
            // 以上都是初始化DetectionUnit，用户要根据特定的Source、Schema等自行配置解析方法。
            .assignTimestampsAndWatermarks(WatermarkStrategy.<DetectUnit>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                .withTimestampAssigner((r, timestamp) -> r.getTimestamp()));

    SlidingEventTimeWindows detectWindow = SlidingEventTimeWindows.of(Time.seconds(detectWindowLen), Time.seconds(detectWindowStep));

    // 异常检测 + 数据赋值。
    SingleOutputStreamOperator<DetectUnit> abnormalStream = source
        .keyBy(DetectUnit::getId)
        .window(detectWindow)
        .process(new AveLowerJudgeDouble(abnormalThreshold));

    // 用户自定义的维表初始化与检测单元信息JOIN
    SingleOutputStreamOperator<DetectUnit> richedStream =
        abnormalStream.map(new UniGridInfoJoiner(rasterSize, partitionGridSize, distTreshhold));

    // do detect
    AreaDetect detect = new AreaDetect(richedStream, detectWindow, new AveGeomKPIGetter());
    DataStream<Geometry> resultStream = detect.process();
    resultStream.addSink(new ResultFileSinkFunction(sinkFile)).setParallelism(1);
    env.execute("abc");
  }
}
