package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.core.combine.centralized.CentralizedCombiner;
import cn.edu.whu.glink.areadetect.core.combine.distributed.HotAreaFinalCombiner;
import cn.edu.whu.glink.areadetect.core.combine.distributed.Local2GlobalMapRuleGetter;
import cn.edu.whu.glink.areadetect.core.combine.distributed.LocalAreaLinkFinder;
import cn.edu.whu.glink.areadetect.core.combine.distributed.RedundantRouter;
import cn.edu.whu.glink.areadetect.datatypes.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 *
 */
public class AreaDetect {

  TumblingEventTimeWindows hotAreaDetectWindow;

  SingleOutputStreamOperator<DetectUnit> source;

  /**
   *
   * @param source 热点监控单元数据流
   * @param hotAreaDetectWindow 热点区域识别空间窗口, 为滚动窗口, 应与用于识别热点监控单元的滑动步长一致.
   */
  public AreaDetect(SingleOutputStreamOperator<DetectUnit> source,
                    TumblingEventTimeWindows hotAreaDetectWindow) {
    this.source = source;
    this.hotAreaDetectWindow = hotAreaDetectWindow;
  }


  public DataStream<HotArea> distributed() {
    final OutputTag<Tuple3<BoundaryID, DetectUnit, AreaID>> borderUnits = new OutputTag<Tuple3<BoundaryID, DetectUnit, AreaID>>("Boundaries") { };
    final OutputTag<HotArea> needCombineTag = new OutputTag<HotArea>("NeedCombineAreas") { };
    SingleOutputStreamOperator<HotArea> localAreas =  localAreaDetect(source);
    DataStream<MapRule> idMapRules = getIdMapRules(localAreas.getSideOutput(borderUnits));
    DataStream<HotArea> globalAreas = alterAreaID(idMapRules, localAreas.getSideOutput(needCombineTag));
    return hotAreaFinalCombine(globalAreas.union(localAreas), new HotAreaFinalCombiner());
  }

  public DataStream<HotArea> centralized() {
    SingleOutputStreamOperator<HotArea> localAreas =  localAreaDetect(source);
    return localAreas.windowAll(hotAreaDetectWindow)
        .process(new CentralizedCombiner());
  }

  /**
   * 热点单元数据流作冗余分区, 以分区网格为单位并行作本地热点区域识别.
   *
   * @param source 热点单元数据流
   * @return 3路输出:
   *    默认输出: 不需要和其他分区热点区域合并的热点区域; <br>
   *    旁路输出: "need combine areas" 中为需要和其他分区内识别的热点区域合并的热点区域;<br>
   *    旁路输出: "border" 中为热点区域ID\分区边界ID\监控单元ID等信息.
   */
  private SingleOutputStreamOperator<HotArea> localAreaDetect(DataStream<DetectUnit> source) {
    return source
        .keyBy(DetectUnit::getId)
        .flatMap(new RedundantRouter())
        .keyBy(f -> f.f0)
        .window(hotAreaDetectWindow)
        .process(new LocalAreaDetect());
  }

  /**
   * 基于Interval join, 将待合并热点区域和适配的Area ID映射规则关联, 修改待合并热点区域的Area ID.
   */
  private DataStream<MapRule> getIdMapRules(DataStream<Tuple3<BoundaryID, DetectUnit, AreaID>> marginUnitStream) {
    return marginUnitStream.keyBy(r -> r.f0).window(hotAreaDetectWindow)
        .process(new LocalAreaLinkFinder())
        .windowAll(hotAreaDetectWindow)
        .process(new Local2GlobalMapRuleGetter());
  }

  /**
   * 将需要合并的热点区域元组<AreaID, List>的Area ID统一
   */
  private DataStream<HotArea> alterAreaID(DataStream<MapRule> fixRuleStream, DataStream<HotArea> toFixStream) {
    return fixRuleStream.keyBy(f -> f.getFrom()).intervalJoin(toFixStream.keyBy(f -> f.getAreaID()))
        .between(Time.seconds(-1), Time.seconds(1))
        .process(new Local2GlobalJoinFunction());
  }

  /**
   * 合并Area ID相同的监控单元列表们, 并输出最终的热点区域Geometry
   */
  private DataStream<HotArea> hotAreaFinalCombine(DataStream<HotArea> hotAreas, HotAreaFinalCombiner hotAreaFinalCombiner) {
    return hotAreas
        .keyBy(f -> f.getAreaID())
        .window(hotAreaDetectWindow)
        .process(hotAreaFinalCombiner);
  }

  private static class Local2GlobalJoinFunction extends ProcessJoinFunction<MapRule, HotArea, HotArea> {

    @Override
    public void processElement(MapRule left, HotArea right, Context ctx, Collector<HotArea> out) throws Exception {
      right.setAreaID(left.getTo());
      out.collect(right);
    }
  }

}
