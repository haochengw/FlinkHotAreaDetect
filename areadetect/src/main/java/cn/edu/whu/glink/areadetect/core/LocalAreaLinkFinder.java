package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.feature.AreaID;
import cn.edu.whu.glink.areadetect.feature.BoundryID;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

/**
 * 输入Tuple3<BoundryID, DetectUnit, AreaID>分别为边界ID、检测单元、检测单元所属的局部区域ID
 */
public class LocalAreaLinkFinder extends ProcessWindowFunction<Tuple3<BoundryID, DetectUnit, AreaID>, Tuple2<AreaID, AreaID>, BoundryID, TimeWindow> {

  @Override
  public void process(BoundryID boundryID, Context context, Iterable<Tuple3<BoundryID, DetectUnit, AreaID>> iterable, Collector<Tuple2<AreaID, AreaID>> collector) throws Exception {
    HashMap<DetectUnit, AreaID> lowerSet = new HashMap<>();
    HashMap<DetectUnit, AreaID> upperSet = new HashMap<>();
    HashSet<LocalAreaConnectPair> pairs = new HashSet<>();
    for (Tuple3<BoundryID, DetectUnit, AreaID> t : iterable) {
      long partition = t.f2.getPartitionID();
      if (partition == boundryID.getSmaller())
        lowerSet.put(t.f1, t.f2);
      else
        upperSet.put(t.f1, t.f2);
    }
    for (Map.Entry<DetectUnit, AreaID> entry : lowerSet.entrySet()) {
      DetectUnit unit1 = entry.getKey();
      AreaID localAreaID1 = entry.getValue();
      if (upperSet.containsKey(unit1)) {
        AreaID localAreaID2 = upperSet.get(unit1);
        if (!Objects.equals(localAreaID1, localAreaID2)) {
          LocalAreaConnectPair pair = new LocalAreaConnectPair(localAreaID1, localAreaID2);
          if (!pairs.contains(pair)) {
            collector.collect(new Tuple2<>(pair.smaller, pair.bigger));
            pairs.add(pair);
          }
        }
      }
    }
  }


  private static class LocalAreaConnectPair {
    AreaID smaller;
    AreaID bigger;

    LocalAreaConnectPair(AreaID localAreaID1, AreaID localAreaID2) {
      long p1 = localAreaID1.getPartitionID();
      long p2 = localAreaID2.getPartitionID();
      if (p1 < p2) {
        smaller = localAreaID1;
        bigger = localAreaID2;
      } else {
        smaller = localAreaID2;
        bigger = localAreaID1;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LocalAreaConnectPair that = (LocalAreaConnectPair) o;
      return (smaller == that.smaller && bigger == that.bigger);
    }

    @Override
    public int hashCode() {
      return Objects.hash(smaller, bigger);
    }
  }
}
