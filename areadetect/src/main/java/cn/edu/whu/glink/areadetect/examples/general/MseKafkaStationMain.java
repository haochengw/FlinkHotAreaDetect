package cn.edu.whu.glink.areadetect.examples.general;

import cn.edu.whu.glink.areadetect.examples.func.AveGeomKPIGetter;
import cn.edu.whu.glink.areadetect.examples.func.AveLowerJudgeDouble;
import cn.edu.whu.glink.index.GeographicalGridIndex;
import cn.edu.whu.glink.areadetect.core.AreaDetect;
import cn.edu.whu.glink.areadetect.examples.IO.sink.ResultFileSinkFunction;
import cn.edu.whu.glink.areadetect.examples.IO.source.MseKafkaDataSource;
import cn.edu.whu.glink.areadetect.examples.IO.source.helper.MseString2StationDetectionUnit;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.areadetect.graph.StationColocReader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;

import java.time.Duration;
import java.util.HashMap;

public class MseKafkaStationMain {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = new StreamExecutionEnvironment();

    // some paths
    double partitionDist = 5;
    String neighborFilePath = "";
    String polygonFilePath = "";
    String stationFilePath = "";
    String overlapStationIDFilePath = "";
    String sinkFile = "";
    HashMap<Long, Long> unitIDMap = StationColocReader.read(overlapStationIDFilePath);

    SingleOutputStreamOperator<DetectUnit> source =
        MseKafkaDataSource.getSource(env)
            .map(new MseString2StationDetectionUnit())
            .map(r -> r.setId(unitIDMap.getOrDefault(r.getId(), r.getId())))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<DetectUnit>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                .withTimestampAssigner((r, timestamp) -> r.getTimestamp()));
    SlidingEventTimeWindows detectWindow = SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(5));
    SingleOutputStreamOperator<DetectUnit> abnormalStream = source
        .keyBy(DetectUnit::getId)
        .window(detectWindow)
        .process(new AveLowerJudgeDouble(0));
    // 用户自定义的维表初始化与检测单元信息JOIN
    SingleOutputStreamOperator<DetectUnit> richedStream =
        abnormalStream.map(new StationInfoJoiner(stationFilePath, neighborFilePath, polygonFilePath, new GeographicalGridIndex(partitionDist)));
    // do detect
    AreaDetect detect = new AreaDetect(richedStream, detectWindow, new AveGeomKPIGetter());
    DataStream<Geometry> resultStream = detect.process();
    resultStream.addSink(new ResultFileSinkFunction(sinkFile)).setParallelism(1);
    env.execute("abc");
  }
}
