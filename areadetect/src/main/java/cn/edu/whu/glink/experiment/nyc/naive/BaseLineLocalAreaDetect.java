package cn.edu.whu.glink.experiment.nyc.naive;

import cn.edu.whu.glink.areadetect.feature.AreaID;
import cn.edu.whu.glink.areadetect.feature.BoundryID;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;


public class BaseLineLocalAreaDetect extends ProcessWindowFunction<Tuple2<Long, DetectUnit>, Tuple2<AreaID, List<DetectUnit>>, Long, TimeWindow> {



  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public void process(Long key, Context context, Iterable<Tuple2<Long, DetectUnit>> iterable, Collector<Tuple2<AreaID, List<DetectUnit>>> out) throws Exception {
    HashMap<Long, DetectUnit> visited = new HashMap<>();
    HashMap<Long, DetectUnit> units = new HashMap<>();
    int count = 0;
    // 转换成id -> 单元，方便查询
    for (Tuple2<Long, DetectUnit> unit : iterable) {
      units.put(unit.f1.getId(), unit.f1);
    }
    for (DetectUnit unit : units.values()) {
      if (visited.containsKey(unit.getId()) || !Objects.equals(unit.getMainPartition(), key))
        continue;
      visited.put(unit.getId(), unit);
      // -----------  a new local area id;
      count++;
      AreaID currAreaID = getLocalAreaID(key, count);
      // ----------- flag indicating to sink or fix。
      boolean needCombine = false;
      // list to collect the data in the local area;
      List<DetectUnit> uninList = new ArrayList<>();
      // do dfs
      Stack<DetectUnit> stack = new Stack<>();
      stack.add(unit);
      while (!stack.isEmpty()) {
        // 栈中的元素一定在这个分区内
        DetectUnit temp = stack.pop();
        uninList.add(temp);
        Set<Long> neighborIDs = temp.getNearByUnitsID();
        List<DetectUnit> neighbors = new LinkedList<>();
        for (Long id : neighborIDs) {
          if (units.containsKey(id)) {
            neighbors.add(units.get(id));
          }
        }
        for (DetectUnit neighbor : neighbors) {
          if (!visited.containsKey(neighbor.getId())) {
            visited.put(neighbor.getId(), neighbor);
            Long partitionOfNeighbor = neighbor.getMainPartition();
            if (Objects.equals(partitionOfNeighbor, key)) { // 如果neighbor中一个点的邻居在分区外，说明这个点本身在边界上，需要把他的邻居发向下游的边界上。
              stack.push(neighbor);
            }
          }
        }
      }
      out.collect(new Tuple2<>(currAreaID, uninList));
    }
  }

  /**
   * 更小的分区id在左侧高位，更低的分区id在低位
   */
  private BoundryID getBorderID(long a, long b) {
    return new BoundryID(a, b);
  }

  private AreaID getLocalAreaID(Long partitionID, int count) {
    return new AreaID(partitionID, count);
  }
}