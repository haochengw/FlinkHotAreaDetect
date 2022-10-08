package cn.edu.whu.glink.areadetect.core.combine.distributed;

import cn.edu.whu.glink.areadetect.datatypes.AreaID;
import cn.edu.whu.glink.areadetect.datatypes.AreaIDLink;
import cn.edu.whu.glink.areadetect.datatypes.MapRule;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 3.4.2 修正规则构建, 映射规则为一个二元组[areaid, targetid] <br/>
 * 其中, areaid为应被合并的热点区域的ID, target为该热点区域ID的修正目标值, 所有应被合并的热点区域的目标ID一致. <br/>
 * 并查集算法
 */
public class Local2GlobalMapRuleGetter extends ProcessAllWindowFunction<AreaIDLink, MapRule, TimeWindow> {
  @Override
  public void process(Context context, Iterable<AreaIDLink> elements, Collector<MapRule> out) throws Exception {
    HashMap<AreaID, AreaID> fatherMap = new HashMap<>();
    for (AreaIDLink t : elements) {
      AreaID s = t.getSmaller();
      AreaID b = t.getBigger();
      AreaID ii = fatherMap.get(s);
      AreaID ij = fatherMap.get(b);
      if (ii != null && ij != null) {
        if (!ii.equals(ij)) {
          fatherMap.entrySet().stream().filter(e -> e.getValue() == ij).forEach(e -> e.setValue(ii));
        }
      } else if (ii != null) {
        fatherMap.put(b, fatherMap.get(s));
      } else if (ij != null) {
        fatherMap.put(s, fatherMap.get(b));
      } else {
        fatherMap.put(s, s);
        fatherMap.put(b, s);
      }
    }
    for (Map.Entry<AreaID, AreaID> entry : fatherMap.entrySet()) {
      AreaID from = entry.getKey();
      AreaID to = entry.getValue();
      while (to != fatherMap.get(to)) {
        to = fatherMap.get(to);
      }
      out.collect(new MapRule(from, to));
    }
  }
}
