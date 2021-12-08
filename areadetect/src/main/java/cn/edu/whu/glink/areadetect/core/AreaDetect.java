package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.feature.AreaID;
import cn.edu.whu.glink.areadetect.feature.BoundryID;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

public class AreaDetect {


  /** 异常区域检测的滑动时间窗口 */
  SlidingEventTimeWindows detectWindowAssigner;

  /** 数据源，其中数据类型为{@link DetectUnit}. */
  SingleOutputStreamOperator<DetectUnit> source;


  protected GeometryKpiGetter geometryKpiGetter;

  public AreaDetect(SingleOutputStreamOperator<DetectUnit> source,
                    SlidingEventTimeWindows detectWindowAssigner,
                    GeometryKpiGetter geometryKpiGetter) {
    this.source = source;
    this.detectWindowAssigner = detectWindowAssigner;
    this.geometryKpiGetter = geometryKpiGetter;
  }


  public DataStream<Geometry> process() {
    final OutputTag<Tuple3<BoundryID, DetectUnit, AreaID>> borderUnits = new OutputTag<Tuple3<BoundryID, DetectUnit, AreaID>>("border") { };
    final OutputTag<Tuple2<AreaID, List<DetectUnit>>> needCombineTag = new OutputTag<Tuple2<AreaID, List<DetectUnit>>>("need combine") { };
    SingleOutputStreamOperator<Tuple2<AreaID, List<DetectUnit>>> localDetectStream = localDetect(source);
    DataStream<Tuple2<AreaID, AreaID>> fixRule = getFixRule(localDetectStream.getSideOutput(borderUnits));
    DataStream<Tuple2<AreaID, List<DetectUnit>>> globalAreas = assignGlobalID(fixRule, localDetectStream.getSideOutput(needCombineTag));
    return getPolygon(globalAreas.union(localDetectStream), new GlobalAreaGeomGetter(geometryKpiGetter));
  }

  /**
   * 3路输出
   */
  private SingleOutputStreamOperator<Tuple2<AreaID, List<DetectUnit>>> localDetect(DataStream<DetectUnit> source) {
    return source
        .keyBy(DetectUnit::getId)
        .flatMap(new RedundantRouter())
        .keyBy(f -> f.f0)
        .window(detectWindowAssigner)
        .process(new LocalAreaDetect());
  }

  private DataStream<Tuple2<AreaID, AreaID>> getFixRule(DataStream<Tuple3<BoundryID, DetectUnit, AreaID>> marginUnitStream) {
    return marginUnitStream.keyBy(r -> r.f0).window(detectWindowAssigner)
        .process(new LocalAreaLinkFinder())
        .windowAll(detectWindowAssigner)
        .process(new Local2GlobalMapRuleGetter());
  }

  private DataStream<Tuple2<AreaID, List<DetectUnit>>> assignGlobalID(DataStream<Tuple2<AreaID, AreaID>> fixRuleStream, DataStream<Tuple2<AreaID, List<DetectUnit>>> toFixStream) {
    return fixRuleStream.keyBy(f -> f.f0).intervalJoin(toFixStream.keyBy(f -> f.f0))
        .between(Time.seconds(-1), Time.seconds(1))
        .process(new Local2GlobalJoinFunction());
  }

  private DataStream<Geometry> getPolygon(DataStream<Tuple2<AreaID, List<DetectUnit>>> partialAreas, GlobalAreaGeomGetter getPolygonFunc) {
    return partialAreas
        .keyBy(f -> f.f0)
        .window(detectWindowAssigner)
        .process(getPolygonFunc);
  }

  public static class Local2GlobalJoinFunction extends ProcessJoinFunction<Tuple2<AreaID, AreaID>, Tuple2<AreaID, List<DetectUnit>>, Tuple2<AreaID, List<DetectUnit>>> {

    @Override
    public void processElement(Tuple2<AreaID, AreaID> left, Tuple2<AreaID, List<DetectUnit>> right, Context ctx, Collector<Tuple2<AreaID, List<DetectUnit>>> out) throws Exception {
      right.f0 = left.f1;
      out.collect(right);
    }
  }

}
