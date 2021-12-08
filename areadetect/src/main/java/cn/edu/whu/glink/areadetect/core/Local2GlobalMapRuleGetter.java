package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.feature.AreaID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class Local2GlobalMapRuleGetter extends ProcessAllWindowFunction<Tuple2<AreaID, AreaID>, Tuple2<AreaID, AreaID>, TimeWindow> {
  @Override
  public void process(Context context, Iterable<Tuple2<AreaID, AreaID>> elements, Collector<Tuple2<AreaID, AreaID>> out) throws Exception {
    HashMap<AreaID, Integer> areaIDtoIndex = new HashMap<>();
    List<List<AreaID>> l = new LinkedList<>();
    for (Tuple2<AreaID, AreaID> t : elements) {  // 1. 两者均出现过, 移动到小的索引中,原来的大的要被清除。
      if (areaIDtoIndex.containsKey(t.f0) && areaIDtoIndex.containsKey(t.f1)) {
        int index0 = areaIDtoIndex.get(t.f0);
        int index1 = areaIDtoIndex.get(t.f1);
        if (index0 != index1) {
          List<AreaID> toMove = l.get(Math.max(index0, index1));
          int targetIndex = Math.min(index0, index1);
          for (AreaID movedAreaIDs : toMove) {
            areaIDtoIndex.put(movedAreaIDs, targetIndex);
          }
          l.get(targetIndex).addAll(toMove);
          toMove.clear();
        }
      } else if (areaIDtoIndex.containsKey(t.f0) || areaIDtoIndex.containsKey(t.f1)) { // 2. 两者出现过其中一个，把未出现过的放到已经出现过了的位置上。
        if (areaIDtoIndex.containsKey(t.f0)) {
          int targetIndex = areaIDtoIndex.get(t.f0);
          l.get(targetIndex).add(t.f1);
          areaIDtoIndex.put(t.f1, targetIndex);
        } else {
          int targetIndex = areaIDtoIndex.get(t.f1);
          l.get(targetIndex).add(t.f0);
          areaIDtoIndex.put(t.f0, targetIndex);
        }
      } else { // 3. 两者都没有出现过
        List<AreaID> toAdd = new LinkedList<>();
        toAdd.add(t.f0);
        toAdd.add(t.f1);
        areaIDtoIndex.put(t.f0, l.size());
        areaIDtoIndex.put(t.f1, l.size());
        l.add(l.size(), toAdd);
      }
    }
    // 为每组分配global id。
    for (List<AreaID> list : l) {
      if (list.size() == 0) continue;
      AreaID globalID = list.get(0);
      for (AreaID localID : list) {
        out.collect(new Tuple2<>(localID, globalID));
      }
    }
  }
}
