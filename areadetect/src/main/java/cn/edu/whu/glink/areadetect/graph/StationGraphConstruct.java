package cn.edu.whu.glink.areadetect.graph;


import cn.edu.whu.glink.index.TRTreeIndex;
import cn.edu.whu.glink.util.GeoUtils;
import org.kynosarges.tektosyne.geometry.PointD;
import org.kynosarges.tektosyne.geometry.Voronoi;
import org.kynosarges.tektosyne.geometry.VoronoiResults;
import org.kynosarges.tektosyne.subdivision.Subdivision;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.io.*;
import java.util.*;


public class StationGraphConstruct {

   PointD[] pointDs;

  // 点 -> 多个同位置基站id组成的list
   HashMap<Point, List<Long>> point2id = new HashMap<>();

  // 基站id -> 点
   HashMap<Long, Point> id2Point = new HashMap<>();

   GeometryFactory gf = new GeometryFactory();

  /**
   * 读取工参文件
   * 以Voronoi图构建每个基站的代表多边形。
   * 以三角网+距离阈值的策略获取每个基站点的邻居节点列表。
   * 工参文件中有较多基站彼此之间经纬度相同、ID不同，以下代码还会有相同经纬度下基站的ID映射规则文件。
   */
  public void main(String[] args) throws IOException {
    // read data
    initFields("/Users/haocheng/IdeaProjects/glink/glink-hw/src/main/resources/dis_par_example.txt");
    
    construct("./stations2polygons.txt", "./stattions2neighbors.txt", "./colocateStations.txt", 5);
  }

  private double getLng(String line) {
    String[] vals = line.split(",");
    return Double.parseDouble(vals[2]);
  }

  private double getLat(String line) {
    String[] vals = line.split(",");
    return Double.parseDouble(vals[3]);
  }

  private long getId(String line) {
    String[] vals = line.split(",");
    return Long.parseLong(vals[0]) / 256;
  }

  public void construct(String stations2Polygons, String stations2neighbors, String overlapStations, double dist) {
    VoronoiResults vr = Voronoi.findAll(pointDs);
    PointD[][] regions = vr.voronoiRegions();
    // id -> polygons
    wirtePolygonData(stations2Polygons, regions);
    // id -> neighbor ids
    Subdivision sd = vr.toDelaunaySubdivision(true);
    writeNeighborData(stations2neighbors, sd, dist);
    writeColocStations(overlapStations);
  }
  
  private  void initFields(String filePath) throws IOException {
    BufferedReader br  = new BufferedReader(new FileReader(filePath));
    List<PointD> pointDList = new LinkedList<>();
    String line;
    while((line = br.readLine())!= null){
      double lng = getLng(line);
      double lat = getLat(line);
      long id = getId(line);
      if(lng == 0 || lat == 0 || id2Point.containsKey(id)) continue;
      PointD pointD = new PointD(lng, lat);
      id2Point.put(id, new Point(new CoordinateArraySequence(new Coordinate[]{new Coordinate(lng, lat)}),gf));
      Point p = new Point(new CoordinateArraySequence(new Coordinate[]{new Coordinate(lng, lat)}),gf);
      if(point2id.containsKey(p)) {
        point2id.get(p).add(id);
      } else {
        pointDList.add(pointD);
        List<Long> list = new LinkedList<>();
        list.add(id);
        point2id.put(p, list);
      }
    }
    pointDs =  pointDList.toArray(new PointD[pointDList.size()]);
  }

  private  void wirtePolygonData(String out, PointD[][] regions) {
    TRTreeIndex<Polygon> index = getPolygonTreeIndex(regions);
    File file = new File(out);
    BufferedWriter bw;
    try {
      bw = new BufferedWriter(new FileWriter(file));
      for (Map.Entry<Point, List<Long>> entry : point2id.entrySet()){
        long id = entry.getValue().get(0);
        Point p = entry.getKey();
        Envelope envelope = p.getEnvelopeInternal();
        List<Polygon> targetPolygons = index.query(envelope);
        if(targetPolygons.size() != 1) {
          for(Polygon targetPolygon : targetPolygons){
            if (targetPolygon.contains(p)) {
              bw.write(id + "|" + targetPolygon);
              bw.newLine();
              break;
            }
          }
        } else {
          bw.write(id + "|" + targetPolygons.get(0));
          bw.newLine();
        }
      }
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private  void writeNeighborData(String out, Subdivision sd, double dist) {
    TRTreeIndex<Point> index = getPointTreeIndex(pointDs);
    try {
      File file = new File(out);
      BufferedWriter bw = new BufferedWriter(new FileWriter(file));
      // 一个点一个记录，每个点的记录的第一位为该点的代表基站ID。
      for(Map.Entry<Point, List<Long>> entry : point2id.entrySet()) {
        List<Long> neighbors = getNeighborPoints(index, entry.getKey(), dist, sd);
        StringJoiner sj = new StringJoiner(",");
        // 第一个ID一定是该点的代表基站ID。
        sj.add(entry.getValue().get(0).toString());
        for(Long neighbor : neighbors) {
          sj.add(neighbor.toString());
        }
        bw.write(sj.toString());
        bw.newLine();
      }
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private  List<Long> getNeighborPoints(TRTreeIndex<Point> index, Point p, double dist, Subdivision sd) {
    List<Long> res = new LinkedList<>();

    // 1. 按照邻近单元代表点间的距离找到邻近点。
    List<Point> temp = new LinkedList<>();
    List<Point> candidates = index.query(GeoUtils.calcEnvelopeByDis(p, dist));
    for (Point candidate : candidates) {
      if(GeoUtils.calcDistance(candidate, p) <= dist) {
        temp.add(candidate);
      }
    }
    // 2. 把邻近点加入到候选集
    HashSet<Point> candidateSet = new HashSet<>(temp);
    // 3. 找三角网中的邻接的点。
    List<PointD> neighborsInGraph = sd.getNeighbors(new PointD(p.getX(), p.getY()));
    for (PointD neighbor : neighborsInGraph) {
      Point neighborP = convert2Point(neighbor);
      candidateSet.add(neighborP);
    }
    candidateSet.remove(p);
    for (Point candidate : candidateSet) {
      res.add(point2id.get(candidate).get(0));
    }
    return res;
  }

  private  TRTreeIndex<Point> getPointTreeIndex(PointD[] pointDs) {
    TRTreeIndex<Point> index = new TRTreeIndex<Point>(pointDs.length);
    for (PointD region : pointDs) {
      index.insert(new Point(new CoordinateArraySequence(new Coordinate[]{new Coordinate(region.x, region.y)}), gf));
    }
    return index;
  }

  private  TRTreeIndex<Polygon> getPolygonTreeIndex(PointD[][] regions) {
    TRTreeIndex<Polygon> index = new TRTreeIndex<Polygon>(regions.length);
    for (PointD[] region : regions) {
      Polygon p = toPolygon(region);
      index.insert(p);
    }
    return index;
  }

  private  Polygon toPolygon(PointD[] region) {
    int size = region.length + 1;
    Coordinate[] vertexs = new Coordinate[size];
    int i = 0;
    for (PointD pointD : region) {
      vertexs[i] = new Coordinate(pointD.x, pointD.y);
      i++;
    }
    vertexs[size - 1] = vertexs[0];
    return new Polygon(new LinearRing(new CoordinateArraySequence(vertexs), gf), null, gf);
  }

  private  Point convert2Point(PointD pointD) {
    return new Point(new CoordinateArraySequence(new Coordinate[]{new Coordinate(pointD.x, pointD.y)}),gf);
  }

  private  void writeColocStations(String out) {
    try {
      File file = new File(out);
      BufferedWriter bw = new BufferedWriter(new FileWriter(file));
      for(Map.Entry<Point, List<Long>> entry : point2id.entrySet()) {
        List<Long> colocStations = entry.getValue();
        StringJoiner sj = new StringJoiner(",");
        for(Long colocStation : colocStations) {
          sj.add(colocStation.toString());
        }
        bw.write(sj.toString());
        bw.newLine();
      }
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
