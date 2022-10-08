package cn.edu.whu.glink.examples.nyc;

import cn.edu.whu.glink.areadetect.core.*;
import cn.edu.whu.glink.areadetect.datatypes.DetectUnit;
import cn.edu.whu.glink.areadetect.datatypes.HotArea;
import cn.edu.whu.glink.areadetect.datatypes.PickDropPoint;
import cn.edu.whu.glink.examples.io.KafkaUtil;
import cn.edu.whu.glink.examples.io.RawRecordParser;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Set;

public class Job {

  static Logger logger = LoggerFactory.getLogger(Job.class);
  static double s = 0.1; // 100米网格
  static long detectWindowLen = 10; // 10分钟滚动窗口
  static long detectWindowSlide = 10; // 10分钟滚动窗口
  static int partitionSize = 1; // 公里
  static String IN_TOPIC = "nyc-2015-01";
  static double thres = 7;
  static String SINKFILE = "2016-01-result";
  static String algo = "dist";

  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public static void main(String[] args) throws Exception {
    doJob(args);
  }


  public static void doJob(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    long start = System.currentTimeMillis();

    if (args.length != 0) {
      s = Double.parseDouble(args[0]);
      detectWindowLen = Integer.parseInt(args[1]);
      detectWindowSlide = Integer.parseInt(args[2]);
      thres = Integer.parseInt(args[3]);
      partitionSize = Integer.parseInt(args[4]);
      IN_TOPIC = args[5];
      algo = args[6];
    }

    SlidingEventTimeWindows slidingWindow = SlidingEventTimeWindows.of(Time.minutes(detectWindowLen), Time.minutes(detectWindowSlide));
    TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(Time.minutes(detectWindowSlide));

    SingleOutputStreamOperator<PickDropPoint> pointStream =
             env.fromSource(KafkaUtil.getKafkaSource(IN_TOPIC),
                             WatermarkStrategy
                                     .<String>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                     .withTimestampAssigner((line, timestamp) -> {
                                       try {
                                         long time = sdf.parse(line.split(",")[0]).getTime();
                                         return time;
                                       } catch (ParseException e) {
                                         logger.error("Parse error found, line is {}", line);
                                         throw new RuntimeException(e);
                                       }
                                     })
                             , "KafkaSource")
                 .map(new RawRecordParser());

    SingleOutputStreamOperator<DetectUnit> mappedStream = pointStream.map(new PointToDetectUnitMapper(s));

    // 热点区域网格识别
    SingleOutputStreamOperator<DetectUnit> hotGridStream = HotDetectUnit.getHotGridStream(mappedStream, slidingWindow, thres, s, partitionSize, s);

    // 热点区域识别, 两种实现方案: 分布式与集中式
    if (algo.equals("dist")) {
      new AreaDetect(hotGridStream, tumblingEventTimeWindows).distributed()
          .map(new PostProcess());
//          .addSink(new cn.edu.whu.glink.examples.io.FileSink(SINKFILE)).setParallelism(1);
    } else if (algo.equals("cent")) {
      new AreaDetect(hotGridStream, tumblingEventTimeWindows).centralized()
          .map(new PostProcess());
//          .addSink(new cn.edu.whu.glink.examples.io.FileSink(SINKFILE)).setParallelism(1);
    }

    env.execute("nyc-job");
    System.out.println(System.currentTimeMillis() - start);
  }


  public static class PostProcess implements MapFunction<HotArea, Geometry> {
    @Override
    public Geometry map(HotArea hotArea) throws Exception {
      // 0. 区域ID 1. 时间 2. 平均值 3. 单元数量
      Geometry geometry = hotArea.getGeometry();
      Tuple tuple = new Tuple5<>();
      tuple.setField(hotArea.getAreaID(), 0);
      tuple.setField(hotArea.getTimestamp(), 1);
      Set<DetectUnit> detectUnits = hotArea.getDetectUnits();
      int count = 0;
      int sum = 0;
      for(DetectUnit unit : detectUnits) {
        count++;
        sum += (Integer) unit.getVal();
      }
      double avg = sum / (double) count;
      tuple.setField(avg, 2);
      tuple.setField(detectUnits.size(), 3);
      geometry.setUserData(tuple);
      return geometry;
    }
  }
}
