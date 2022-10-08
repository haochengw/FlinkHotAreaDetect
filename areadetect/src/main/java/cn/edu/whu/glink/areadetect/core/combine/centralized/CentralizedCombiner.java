package cn.edu.whu.glink.areadetect.core.combine.centralized;

import cn.edu.whu.glink.areadetect.datatypes.AreaID;
import cn.edu.whu.glink.areadetect.datatypes.HotArea;
import cn.edu.whu.glink.areadetect.index.TRTreeIndex;
import cn.edu.whu.glink.areadetect.index.TreeIndex;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

import java.util.*;

/**
 * Centralized Baseline, 借助R-Tree实现热点区域的合并
 * @author Haocheng Wang
 * Created on 2022/10/7
 */
public class CentralizedCombiner extends ProcessAllWindowFunction<HotArea, HotArea, TimeWindow> {

  @Override
  public void process(Context context, Iterable<HotArea> iterable, Collector<HotArea> collector) throws Exception {
    Iterator<HotArea> iterator = iterable.iterator();
    TreeIndex<Geometry> treeIndex = new TRTreeIndex<>();
    HashSet<Geometry> set = new HashSet<>();
    while (iterator.hasNext()) {
      HotArea area = iterator.next();
      Geometry geom = area.getGeometry();
      treeIndex.insert(geom);
      set.add(area.getGeometry());
    }
    // bfs merge
    Deque<Geometry> deque = new LinkedList<>();
    Map<AreaID, List<Geometry>> map = new HashMap<>();
    HashSet<Geometry> visited = new HashSet<>();
    for (Geometry geometry : set) {
      // 未被访问过, 开始bfs合并过程
      if (!visited.contains(geometry)) {
        visited.add(geometry);
        treeIndex.remove(geometry);
        deque.offerLast(geometry);
        HotArea area = (HotArea) geometry.getUserData();
        AreaID areaID = area.getAreaID();
        map.put(areaID, new ArrayList<>(Collections.singletonList(geometry)));
        while (!deque.isEmpty()) {
          int size = deque.size();
          for (int i = 0; i < size; i++) {
            Geometry geom = deque.pollFirst();
            // 查询周边的几何
            List<Geometry> list = treeIndex.query(geometry.buffer(0.00001));
            for (Geometry neighbor : list) {
              deque.offerLast(neighbor);
              visited.add(geom);
              treeIndex.remove(geom);
              map.get(areaID).add(neighbor);
             }
          }
        }
        // bfs结束, 合并
        List<Geometry> list = map.get(areaID);
        HotArea finalArea = (HotArea) list.get(0).getUserData();
        for (Geometry g : list) {
          finalArea.merge((HotArea) g.getUserData());
        }
        collector.collect(finalArea);
      }
    }
  }
}
