package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.datatypes.DetectUnit;
import cn.edu.whu.glink.areadetect.index.GeographicalGridIndex;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashSet;
import java.util.Set;

public class UniGridInfoJoiner implements MapFunction<DetectUnit, DetectUnit> {

  double s;
  double pSize;
  double distTreshhold;

  public UniGridInfoJoiner(double rasterSize, double partitionSize, double distTreshhold) {
    this.s = rasterSize;
    this.pSize = partitionSize;
    this.distTreshhold = distTreshhold;
  }

  @Override
  public DetectUnit map(DetectUnit value) throws Exception {
    GeographicalGridIndex gridIndex = new GeographicalGridIndex(s);
    GeographicalGridIndex partitionIndex = new GeographicalGridIndex(pSize);
    // set lng lat
    long id = value.getId();
    double[] lngLat = gridIndex.getGridCenter(value.getId());
    // near by units;
    HashSet<Long> neighbors = new HashSet<>(gridIndex.kRing(id, (int) Math.floor(distTreshhold / s)));
    // near by partitions;
    Set<Long> nearByPartitions = new HashSet<>();
    for (Long neighbor : neighbors) {
      double[] lngLatNeighbor = gridIndex.getGridCenter(neighbor);
      nearByPartitions.add(partitionIndex.getIndex(lngLatNeighbor[0], lngLatNeighbor[1]));
    }
    long mainPartition = partitionIndex.getIndex(lngLat[0], lngLat[1]);
    nearByPartitions.remove(mainPartition);
    // add info
    value.setMainPartition(mainPartition);
    value.setPolygon(gridIndex.getGridPolygon(id));
    value.setNearByPartitions(nearByPartitions);
    value.setNearByUnitsID(neighbors);
    return value;
  }
}
