package cn.edu.whu.glink.examples.wuhan;

import cn.edu.whu.glink.areadetect.core.AreaDetect;
import cn.edu.whu.glink.areadetect.core.HotDetectUnit;
import cn.edu.whu.glink.areadetect.core.PointToDetectUnitMapper;
import cn.edu.whu.glink.areadetect.datatypes.DetectUnit;
import cn.edu.whu.glink.areadetect.datatypes.PickDropPoint;
import cn.edu.whu.glink.examples.io.FileSource;
import cn.edu.whu.glink.examples.io.RawRecordParser;
import cn.edu.whu.glink.examples.shp.ShapeFileUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.StringJoiner;

/**
 * @author Haocheng Wang
 * Created on 2022/10/8
 */
public class Job {
  static double s = 0.1; // 100米网格
  static long detectWindowLen = 10; // 10分钟滚动窗口
  static long detectWindowSlide = 10; // 10分钟滚动窗口
  static int partitionSize = 10; // 公里
  static double THRESHOLD = 7;
  static String SINK_FILE;
  static String PICK_DROP_FILE_PATH;
  static String SHP_RESULT_FOLDER_PATH;

  public static void main(String[] args) throws Exception {
    SINK_FILE = getSinkFileName();
    doJob(args);
  }


  public static void doJob(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    long start = System.currentTimeMillis();

    SlidingEventTimeWindows slidingWindow = SlidingEventTimeWindows.of(Time.minutes(detectWindowLen), Time.minutes(detectWindowSlide));
    TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(Time.minutes(detectWindowSlide));

    SingleOutputStreamOperator<PickDropPoint> pointStream =
        env.addSource(new FileSource(PICK_DROP_FILE_PATH))
            .map(new RawRecordParser())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<PickDropPoint>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                .withTimestampAssigner((pickDropPoint, l) -> pickDropPoint.getTime()));

    SingleOutputStreamOperator<DetectUnit> mappedStream = pointStream.map(new PointToDetectUnitMapper(s));

    // 热点区域网格识别
    SingleOutputStreamOperator<DetectUnit> hotGridStream = HotDetectUnit.getHotGridStream(mappedStream, slidingWindow, THRESHOLD, s, partitionSize, s);

    // 热点区域识别
    new AreaDetect(hotGridStream, tumblingEventTimeWindows).distributed()
        .map(new cn.edu.whu.glink.examples.nyc.Job.PostProcess())
        .addSink(new cn.edu.whu.glink.examples.io.FileSink(SINK_FILE)).setParallelism(1);

    env.execute("nyc-job");
    System.out.println(System.currentTimeMillis() - start);
    ShapeFileUtil.createShp(SINK_FILE, SHP_RESULT_FOLDER_PATH);
  }

  private static String getSinkFileName() {
    StringJoiner sj = new StringJoiner("-");
    sj.add("hotAreas")
        .add("windowLen")
        .add(String.valueOf(detectWindowLen))
        .add("windowSlide")
        .add(String.valueOf(detectWindowSlide))
        .add("s")
        .add(String.valueOf(s))
        .add("v")
        .add(String.valueOf(THRESHOLD));
    return sj.toString();
  }
}
