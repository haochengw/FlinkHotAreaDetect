package cn.edu.whu.glink.experiment.shenzhen;

import cn.edu.whu.glink.areadetect.core.AreaDetect;
import cn.edu.whu.glink.areadetect.core.GeometryKpiGetter;
import cn.edu.whu.glink.areadetect.examples.grid.UniGridInfoJoiner;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.index.GeographicalGridIndex;
import cn.edu.whu.glink.experiment.nyc.FileSink;
import cn.edu.whu.glink.experiment.shenzhen.thresholdcal.StatFileSink;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

public class Common {

    public static double rasterSize; // 100米网格
    public static long detectWindowLen = 10; // 10分钟滚动窗口
    public static int partitionSize = 10; // 公里
    public static int threshold;

    public static SlidingEventTimeWindows slidingWindow = SlidingEventTimeWindows.of(Time.minutes(detectWindowLen), Time.minutes(detectWindowLen));

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void init() {
        threshold = Pipeline.threshold;
        rasterSize = Pipeline.RasterSize;
    }



    public static void detect(SingleOutputStreamOperator<Tuple2<DetectUnit, Boolean>> raw, long start) throws Exception {
        // 热点区域网格识别
        SingleOutputStreamOperator<DetectUnit> hotGridStream = getHotGridStream(raw, slidingWindow, threshold, rasterSize, partitionSize, rasterSize);
        // 热点区域识别
        new AreaDetect(hotGridStream,  SlidingEventTimeWindows.of(Time.minutes(detectWindowLen), Time.minutes(detectWindowLen)), new AveHeatVal()).process().addSink(
                new FileSink("hot-" + getSinkFileName())).setParallelism(1);
        env.execute("abc");

        System.out.println(System.currentTimeMillis() - start);
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

    public static String stats(SingleOutputStreamOperator<Tuple2<DetectUnit, Boolean>> raw) throws Exception {
        env.setParallelism(1);
        String sinkFileName = "stats-"+ getSinkFileName();
        // 热点区域网格识别
        SingleOutputStreamOperator<DetectUnit> hotGridStream = getHotGridStream(raw, slidingWindow, 1, rasterSize, partitionSize, rasterSize);
        // 统计
        hotGridStream.map(r -> new Tuple2<>(r, 1)).setParallelism(1).returns(Types.TUPLE(Types.GENERIC(DetectUnit.class), Types.INT))
                .keyBy(r -> r.f0.dropDownCount + r.f0.pickUpCount)
                .window(TumblingEventTimeWindows.of(Time.days(365)))
                .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1)).setParallelism(1).returns(Types.TUPLE(Types.GENERIC(DetectUnit.class), Types.INT))
                .map(r -> new Tuple2<>(r.f0.pickUpCount + r.f0.dropDownCount, r.f1)).setParallelism(1).returns(Types.TUPLE(Types.LONG, Types.INT))
                .addSink(new StatFileSink(sinkFileName)).setParallelism(1);
        env.execute("abc");
        sortAndRewriteFile(sinkFileName);
        return sinkFileName;
    }

    public static String getSinkFileName() {
        StringJoiner sj = new StringJoiner("-");
        sj.add("window" + detectWindowLen)
                .add("gSize" + rasterSize);
        return sj.toString();
    }

    public static SingleOutputStreamOperator<DetectUnit> getHotGridStream(DataStream<Tuple2<DetectUnit, Boolean>> input,
                                                                          SlidingEventTimeWindows slidingWindow,
                                                                          double thres,
                                                                          double rasterSize,
                                                                          double partitionSize,
                                                                          double distThreshold) {
        return input.map((MapFunction<Tuple2<DetectUnit, Boolean>, DetectUnit>) value -> {
                    value.f0.setPickUp(value.f1);
                    return value.f0;
                })
                .keyBy(DetectUnit::getId)
                .window(slidingWindow)
                .aggregate(new SumCountAggregator())
                .filter(r -> r.pickUpCount + r.dropDownCount >= thres)
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
            long timestamp = Long.parseLong(tokens[2]);
            double lng = Double.parseDouble(tokens[0]);
            double lat = Double.parseDouble(tokens[1]);
            boolean isPickUp = tokens[3].equals("true");
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
            a.pickUpCount += b.pickUpCount;
            a.dropDownCount += b.dropDownCount;
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
