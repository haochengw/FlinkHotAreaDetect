package cn.edu.whu.glink.areadetect.examples.grid;

import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.index.GeographicalGridIndex;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UniGridInfoJoiner implements MapFunction<DetectUnit, DetectUnit> {

  double rasterSize;
  double partitionGridSize;
  double distTreshhold;

  public UniGridInfoJoiner(double rasterSize, double partitionSize, double distTreshhold) {
    this.rasterSize = rasterSize;
    this.partitionGridSize = partitionSize;
    this.distTreshhold = distTreshhold;
  }

  @Override
  public DetectUnit map(DetectUnit value) throws Exception {
        GeographicalGridIndex gridIndex = new GeographicalGridIndex(rasterSize);
        GeographicalGridIndex partitionIndex = new GeographicalGridIndex(partitionGridSize);
        // set lng lat
        long id = value.getId();
        double[] lngLat = gridIndex.getGridCenter(value.getId());
        // near by units;
        HashSet<Long> neighbors = new HashSet<>(gridIndex.kRing(id, 1));
        // near by partitions;
         long mainPartition = partitionIndex.getIndex(lngLat[0], lngLat[1]);
        Set<Long> nearByPartitions = new HashSet<>(partitionIndex.kRing(mainPartition, 1));
        nearByPartitions.remove(mainPartition);
        // add info
        value.setMainPartition(mainPartition);
        value.setPolygon(gridIndex.getGridPolygon(id));
        value.setNearByPartitions(nearByPartitions);
        value.setNearByUnitsID(neighbors);
        return value;
  }
}
