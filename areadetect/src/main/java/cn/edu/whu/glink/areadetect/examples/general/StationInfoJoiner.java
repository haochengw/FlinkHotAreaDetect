package cn.edu.whu.glink.areadetect.examples.general;

import cn.edu.whu.glink.areadetect.examples.IO.source.helper.UnitPointReader;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.areadetect.graph.NeighborReader;
import cn.edu.whu.glink.areadetect.graph.PolygonReader;
import cn.edu.whu.glink.index.GeographicalGridIndex;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * 使用MapFunction + 状态，预加载维表，在每条数据到达时作数据填充。
 */
public class StationInfoJoiner extends RichMapFunction<DetectUnit, DetectUnit> {

  String pointFilePath;
  String neighborFilePath;
  String polygonFilePath;

  HashMap<Long, Polygon> polygonHashMap;
  HashMap<Long, HashSet<Long>> neighborHashMap;
  HashMap<Long, Point> pointHashMap;
  GeographicalGridIndex partitionIndex;

  public StationInfoJoiner(String pointFilePath, String neighborFilePath, String polygonFilePath, GeographicalGridIndex partitionIndex) {
    this.pointFilePath = pointFilePath;
    this.neighborFilePath = neighborFilePath;
    this.polygonFilePath = polygonFilePath;
    this.partitionIndex = partitionIndex;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    polygonHashMap = PolygonReader.read(polygonFilePath);
    neighborHashMap = NeighborReader.read(neighborFilePath);
    pointHashMap = UnitPointReader.read(pointFilePath);
  }

  @Override
  public DetectUnit map(DetectUnit value) throws Exception {
    long id = value.getId();
    HashSet<Long> neighborIDs = neighborHashMap.get(value.getId());
    Set<Long> nearByPartitions = new HashSet<>();
    for (Long neighbor : neighborHashMap.keySet()) {
      Point p = pointHashMap.get(neighbor);
      nearByPartitions.add(partitionIndex.getIndex(p.getX(), p.getY()));
    }
    Point p = pointHashMap.get(id);
    long mainPartition = partitionIndex.getIndex(p.getX(), p.getY());
    nearByPartitions.remove(mainPartition);
    // add info
    value.setMainPartition(mainPartition);
    value.setNearByPartitions(nearByPartitions);
    value.setPolygon(polygonHashMap.get(id));
    value.setNearByUnitsID(neighborIDs);
    return value;
  }
}