package cn.edu.whu.glink.examples.threshold;

import cn.edu.whu.glink.areadetect.core.HotDetectUnit;
import cn.edu.whu.glink.areadetect.core.PointToDetectUnitMapper;
import cn.edu.whu.glink.areadetect.datatypes.DetectUnit;
import cn.edu.whu.glink.areadetect.datatypes.PickDropPoint;
import cn.edu.whu.glink.examples.io.FileSource;
import cn.edu.whu.glink.examples.io.RawRecordParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringJoiner;


public class ThresholdJob {

  static Logger logger = LoggerFactory.getLogger(ThresholdJob.class);
  static double s;
  static int aggWindowLen;
  static int aggWindowSlide;
  static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  static String FILE_PATH = "pickOrDropSorted.txt";

  public static void main(String[] args) {
    System.out.println(getThreshold(FILE_PATH));
  }

  public static int getThreshold(String inputFile) {
    try {
      String statsFile = getStatsFile(inputFile);
      logger.info("The stats file name is {}", statsFile);
      return ThresholdCalculator.getThreshold(statsFile);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 0;
  }

  private static String getStatsFile(String inputFile) throws Exception {
    SingleOutputStreamOperator<PickDropPoint> pointStream =
        env.addSource(new FileSource(inputFile))
            .map(new RawRecordParser())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<PickDropPoint>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                .withTimestampAssigner((pickDropPoint, l) -> pickDropPoint.getTime()));
    return stats(pointStream);
  }

  /**
   * 以时间窗口和监控单元为统计单元, 统计每个热度值的出现频次, 并按照热度值从小到大排列, 运用单峰阈值选取法.
   */
  private static String stats(SingleOutputStreamOperator<PickDropPoint> pointStream) throws Exception {
    env.setParallelism(1);
    String sinkFileName = getSinkFileName();
    // 映射
    SingleOutputStreamOperator<DetectUnit> mappedStream = pointStream.map(new PointToDetectUnitMapper(s));

    // 热点网格识别
    SingleOutputStreamOperator<DetectUnit> hotGridStream = HotDetectUnit.getHotGridStream(mappedStream,
        SlidingEventTimeWindows.of(Time.minutes(aggWindowLen), Time.minutes(aggWindowSlide)),
        1,
        s,
        20.0,
        s);

    // 统计不同上下客累积值的数量
    hotGridStream.map(r -> new Tuple2<>(r, 1)).setParallelism(1).returns(Types.TUPLE(Types.GENERIC(DetectUnit.class), Types.INT))
        .keyBy(r -> r.f0.getVal())
        .window(TumblingEventTimeWindows.of(Time.days(365)))
        .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1)).setParallelism(1).returns(Types.TUPLE(Types.GENERIC(DetectUnit.class), Types.INT))
        .map(r -> new Tuple2<>(r.f0.getVal(), r.f1)).setParallelism(1).returns(Types.TUPLE(Types.INT, Types.INT))
        .addSink(new StatFileSink(sinkFileName)).setParallelism(1);
    env.execute(sinkFileName);
    sortAndRewriteFile(sinkFileName);
    return sinkFileName;
  }

  private static String getSinkFileName() {
    StringJoiner sj = new StringJoiner("-");
    sj.add("threshold")
        .add("window")
        .add(String.valueOf(aggWindowLen))
        .add("s")
        .add(String.valueOf(s));
    return sj.toString();
  }

  private static void sortAndRewriteFile(String filePath) throws IOException {
    ArrayList<int[]> list = new ArrayList<>();
    BufferedReader br = new BufferedReader(new FileReader(filePath));
    String line;
    while ((line = br.readLine()) != null) {
      String[] tokens = line.split(",");
      int[] arr = new int[2];
      arr[0] = Integer.parseInt(tokens[0]);
      arr[1] = Integer.parseInt(tokens[1]);
      list.add(arr);
    }
    Collections.sort(list, (a, b) -> a[0] - b[0]);
    BufferedWriter bw = new BufferedWriter(new FileWriter(filePath));
    for (int[] a :list) {
      bw.write(a[0] + "," + a[1]);
      bw.newLine();
    }
    bw.flush();
  }

  public static class StatFileSink implements SinkFunction<Tuple2<Integer, Integer>> {

    transient BufferedWriter br;
    String fileName;

    public StatFileSink(String fileName) {
      this.fileName = fileName;
    }

    public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
      if(br == null) {
        br = new BufferedWriter(new FileWriter(fileName));
      }
      StringJoiner sj = new StringJoiner(",");
      sj.add(String.valueOf(value.f0));
      sj.add(String.valueOf(value.f1));
      br.write(sj.toString());
      br.newLine();
      br.flush();
    }
  }
}
