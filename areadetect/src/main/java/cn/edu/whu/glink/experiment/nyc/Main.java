package cn.edu.whu.glink.experiment.nyc;

import cn.edu.whu.glink.areadetect.core.AreaDetect;
import cn.edu.whu.glink.areadetect.core.GeometryKpiGetter;
import cn.edu.whu.glink.areadetect.examples.grid.UniGridInfoJoiner;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.index.GeographicalGridIndex;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.StringJoiner;

public class Main {
  static double rasterSize = 0.1; // 100米网格
  static long detectWindowLen = 10; // 10分钟滚动窗口
  static int partitionSize = 1; // 公里
  static String TOPIC = "nyc-2016-01";
  static double thres = 7;
  static String SINKFILE = "2016-01-result";


  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    long start = System.currentTimeMillis();

//    rasterSize = Double.parseDouble(args[0]);
//    detectWindowLen = Integer.parseInt(args[1]);
//    thres = Integer.parseInt(args[2]);
//    partitionSize = Integer.parseInt(args[3]);
//    TOPIC = args[4];

    SlidingEventTimeWindows slidingWindow = SlidingEventTimeWindows.of(Time.minutes(detectWindowLen), Time.minutes(detectWindowLen));
    SingleOutputStreamOperator<Tuple2<DetectUnit, Boolean>> raw =
        env.fromSource(getKafkaSource(TOPIC), WatermarkStrategy.noWatermarks(), "KafkaSource")
        .map(new Parser(rasterSize))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<DetectUnit, Boolean>>forBoundedOutOfOrderness(Duration.ofMinutes(5))
            .withTimestampAssigner((r, timestamp) -> r.f0.getTimestamp()  + 28800000));

    // 热点区域网格识别
    SingleOutputStreamOperator<DetectUnit> hotGridStream = getHotGridStream(raw, slidingWindow, thres, rasterSize, partitionSize, rasterSize);

    // 热点区域识别
    new AreaDetect(hotGridStream, slidingWindow, new AveHeatVal()).process().addSink(new FileSink(SINKFILE)).setParallelism(1);

    env.execute("abc");
    System.out.println(System.currentTimeMillis() - start);
  }

  private static String getSinkFileName(long wSize, double gSize) {
    StringJoiner sj = new StringJoiner("-");
    sj.add("window" + Long.toString(wSize))
        .add("gSize" + Double.toString(gSize));
    return sj.toString();
  }

  public static KafkaSource<String> getKafkaSource(String topicName) {
    return KafkaSource.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics(topicName)
        .setGroupId("abc")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setBounded(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema()).build();
  }

  public static SingleOutputStreamOperator<DetectUnit>  getHotGridStream(DataStream<Tuple2<DetectUnit, Boolean>> input, SlidingEventTimeWindows slidingWindow, double thres
      , double rasterSize, double partitionSize, double distThreshold) {
    return input.map((MapFunction<Tuple2<DetectUnit, Boolean>, DetectUnit>) value -> {
      value.f0.setPickUp(value.f1);
      return value.f0;
    })
        .keyBy(DetectUnit::getId)
        .window(slidingWindow)
        .aggregate(new SumCountAggregator())
        .filter(r -> (Integer) r.getVal() >= thres)
        .map(new UniGridInfoJoiner(rasterSize, partitionSize, distThreshold));
  }

  /**
   * input format: 'row-id, datetime, lng, lat, type'
   */
  public static class Parser implements MapFunction<String, Tuple2<DetectUnit, Boolean>> {

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final GeographicalGridIndex gridIndex;

    public Parser(double rasterSize) {
      gridIndex = new GeographicalGridIndex(rasterSize);
    }

    @Override
    public Tuple2<DetectUnit, Boolean> map(String value) throws Exception {
      String[] tokens = value.split(",");
      Date dateTime = sdf.parse(tokens[1]);
      long timestamp = dateTime.getTime();
      double lng = Double.parseDouble(tokens[2]);
      double lat = Double.parseDouble(tokens[3]);
      int type = Integer.parseInt(tokens[4]);
      boolean isPickUp = type == 0;
      return new Tuple2<>(new DetectUnit(gridIndex.getIndex(lng, lat), timestamp, 1), isPickUp);
    }
  }

  public static class SumCountAggregator implements AggregateFunction<DetectUnit, DetectUnit, DetectUnit> {

    @Override
    public DetectUnit createAccumulator() {
      return null;
    }

    @Override
    public DetectUnit add(DetectUnit value, DetectUnit accumulator) {
      if(accumulator == null) {
        accumulator = new DetectUnit(value.getId(), value.getTimestamp(), value.getVal());
      } else {
        accumulator.setVal((Integer) accumulator.getVal() + 1);
      }
      if(value.isPickUp) {
        accumulator.pickUpCount = accumulator.pickUpCount + 1;
      } else {
        accumulator.dropDownCount = accumulator.dropDownCount + 1;
      }
      return accumulator;
    }

    @Override
    public DetectUnit getResult(DetectUnit accumulator) {
      return accumulator;
    }

    @Override
    public DetectUnit merge(DetectUnit a, DetectUnit b) {
      a.setVal((Integer) a.getVal() + (Integer) b.getVal());
      return a;
    }
  }

  public static class AveHeatVal implements GeometryKpiGetter {

    @Override
    public Double get(List<DetectUnit> detectUnits) {
      int count = 0;
      int sum = 0;
      for(DetectUnit unit : detectUnits) {
        count++;
        sum += (Integer) unit.getVal();
      }
      return sum / (double) count;
    }
  }
}
