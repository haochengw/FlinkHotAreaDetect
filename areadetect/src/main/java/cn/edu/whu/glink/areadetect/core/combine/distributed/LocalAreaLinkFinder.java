package cn.edu.whu.glink.areadetect.core.combine.distributed;

import cn.edu.whu.glink.areadetect.datatypes.AreaID;
import cn.edu.whu.glink.areadetect.datatypes.AreaIDLink;
import cn.edu.whu.glink.areadetect.datatypes.BoundaryID;
import cn.edu.whu.glink.areadetect.datatypes.DetectUnit;
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
 * 3.4.1 链接识别 <br/>
 * 输入Tuple3<BoundryID, DetectUnit, AreaID>分别为边界ID、检测单元、检测单元所属的局部区域ID, 输出应被合并的两个热点区域的ID对.
 */
public class LocalAreaLinkFinder extends ProcessWindowFunction<Tuple3<BoundaryID, DetectUnit, AreaID>, AreaIDLink, BoundaryID, TimeWindow> {

  @Override
  public void process(BoundaryID boundaryID, Context context, Iterable<Tuple3<BoundaryID, DetectUnit, AreaID>> iterable, Collector<AreaIDLink> collector) throws Exception {
    HashMap<DetectUnit, AreaID> lowerSet = new HashMap<>();
    HashMap<DetectUnit, AreaID> upperSet = new HashMap<>();
    HashSet<AreaIDLink> links = new HashSet<>();
    for (Tuple3<BoundaryID, DetectUnit, AreaID> t : iterable) {
      long partition = t.f2.getPartitionID();
      if (partition == boundaryID.getSmaller())
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
          AreaIDLink areaIDLink = new AreaIDLink(localAreaID1, localAreaID2);
          if (!links.contains(areaIDLink)) {
            collector.collect(areaIDLink);
            links.add(areaIDLink);
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
