package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.datatypes.AreaID;
import cn.edu.whu.glink.areadetect.datatypes.BoundaryID;
import cn.edu.whu.glink.areadetect.datatypes.DetectUnit;
import cn.edu.whu.glink.areadetect.datatypes.HotArea;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

/**
 * 3.3 区域识别 <br/>
 * 以分区网格为单位, 基于网格生长算法, 并行作热点区域识别, 此处得到的热点区域中,
 * 一部分可以直接输出,还有一些位于分区网格边缘的热点区域需要后续的全局合并.
 */
public class LocalAreaDetect extends ProcessWindowFunction<Tuple2<Long, DetectUnit>, HotArea, Long, TimeWindow> {

  // 边界区域ID，检测单元信息，局部区域ID。
  OutputTag<Tuple3<BoundaryID, DetectUnit, AreaID>> boundariesTag;
  // 局部区域ID，局部区域中的全部检测单元。
  OutputTag<HotArea> needCombineTag;

  @Override
  public void open(Configuration parameters) throws Exception {
    boundariesTag = new OutputTag<Tuple3<BoundaryID, DetectUnit, AreaID>>("Boundaries") { };
    needCombineTag = new OutputTag<HotArea>("NeedCombineAreas") { };
    super.open(parameters);
  }

  @Override
  public void process(Long key, Context context, Iterable<Tuple2<Long, DetectUnit>> iterable, Collector<HotArea> out) throws Exception {
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
      AreaID currAreaID = getLocalAreaID(key, count, context.window().getEnd());
      // ----------- flag indicating to sink or fix。
      boolean needCombine = false;
      // list to collect the data in the local area;
      Set<DetectUnit> uninList = new HashSet<>();
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
            if (!Objects.equals(partitionOfNeighbor, key)) { // 如果neighbor中一个点的邻居在分区外，说明这个点本身在边界上，需要把他的邻居发向下游的边界上。
              needCombine = true;
              context.output(boundariesTag, new Tuple3<>(getBorderID(key, partitionOfNeighbor), temp, currAreaID));
              context.output(boundariesTag, new Tuple3<>(getBorderID(key, partitionOfNeighbor), neighbor, currAreaID));
            } else {
              stack.push(neighbor);
            }
          }
        }
      }
      if (!needCombine) {
        out.collect(new HotArea(uninList, currAreaID));
      } else {
        context.output(needCombineTag, new HotArea(uninList, currAreaID));
      }
    }
  }

  /**
   * 更小的分区id在左侧高位，更低的分区id在低位
   */
  private BoundaryID getBorderID(long a, long b) {
    return new BoundaryID(a, b);
  }

  private AreaID getLocalAreaID(Long partitionID, int count, long time) {
    return new AreaID(partitionID, count, time);
  }
}