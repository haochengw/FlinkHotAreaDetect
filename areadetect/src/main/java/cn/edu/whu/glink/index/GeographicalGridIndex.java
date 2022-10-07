package cn.edu.whu.glink.index;

import cn.edu.whu.glink.util.GeoUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Grid division based on geographic coordinate system.
 *
 * @author Yu Liebing
 */
public class GeographicalGridIndex extends GridIndex {

  private static final int MAX_BITS = 30;

  private double minLat = -90.0;
  private double maxLat = 90.0;
  private double minLng = -180.0;
  private double maxLng = 180.0;

  private double latWidth;
  private double lngWidth;

  public GeographicalGridIndex(int res) {
    if (res <= 0 || res > 30) {
      throw new IllegalArgumentException("Resolution of grid index must in [1, 30]");
    }
    this.res = res;
    int splits = (int) Math.pow(2, res);
    lngWidth = (maxLng - minLng) / splits;
    latWidth = (maxLat - minLat) / splits;
  }

  public GeographicalGridIndex(Envelope envelope, double lngDistance, double latDistance) {
    this.minLng = envelope.getMinX();
    this.maxLng = envelope.getMaxX();
    this.minLat = envelope.getMinY();
    this.maxLat = envelope.getMaxY();
    lngWidth = GeoUtils.distanceToDEG(lngDistance);
    latWidth = GeoUtils.distanceToDEG(latDistance);
  }

  public GeographicalGridIndex(Envelope envelope, int lngSplits, int latSplits) {
    this(
            envelope.getMinX(),
            envelope.getMaxX(),
            envelope.getMinY(),
            envelope.getMaxY(),
            lngSplits,
            latSplits);
  }

  public GeographicalGridIndex(double minLng, double maxLng,
                               double minLat, double maxLat,
                               int lngSplits, int latSplits) {
    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLng = minLng;
    this.maxLng = maxLng;
    lngWidth = (maxLng - minLng) / lngSplits;
    latWidth = (maxLat - minLat) / latSplits;
  }

  public GeographicalGridIndex(double gridDistance) {
    double deg = GeoUtils.distanceToDEG(gridDistance);
    lngWidth = deg;
    latWidth = deg;
  }

  @Override
  public int getRes() {
    return 0;
  }

  @Override
  public long getIndex(double lng, double lat) {
    long x = (long) ((lng - minLng) / lngWidth);
    long y = (long) ((lat - minLat) / latWidth);
    return combineXY(x, y);
  }

  public double[] getGridCenter(long id){
    long[] xy = this.getXY(id);
    double[] res = new double[2];
    res[0] = xy[0] * lngWidth + minLng;
    res[1] = xy[1] * latWidth + minLat;
    return res;
  }

  @Override
  public List<Long> getIndex(Envelope envelope) {
    long minX = (long) ((envelope.getMinX() - minLng) / lngWidth);
    long maxX = (long) ((envelope.getMaxX() - minLng) / lngWidth);
    long minY = (long) ((envelope.getMinY() - minLat) / latWidth);
    long maxY = (long) ((envelope.getMaxY() - minLat) / latWidth);
    List<Long> indexes = new ArrayList<>((int) ((maxX - minX + 1) * (maxY - minY + 1)));
    for (long x = minX; x <= maxX; ++x) {
      for (long y = minY; y <= maxY; ++y) {
        indexes.add(combineXY(x, y));
      }
    }
    return indexes;
  }

  @Override
  public List<Long> getIndex(Geometry geom) {
    if (geom instanceof Point) {
      Point p = (Point) geom;
      long index = getIndex(p.getX(), p.getY());
      return new ArrayList<Long>(1) {{ add(index); }};
    } else {
      return getIntersectIndex(geom);
    }
  }

  @Override
  public List<Long> getIndex(double lng, double lat, double distance) {
    Envelope envelope = GeoUtils.calcEnvelopeByDis(lng, lat, distance);
    return getIndex(envelope);
  }

  @Override
  public List<Long> getIndex(double lng, double lat, double distance, boolean reduce) {
    if (!reduce) {
      return getIndex(lng, lat, distance);
    }
    Envelope envelope = GeoUtils.calcEnvelopeByDis(lng, lat, distance);
    long minX = (long) ((envelope.getMinX() - minLng) / lngWidth);
    long maxX = (long) ((envelope.getMaxX() - minLng) / lngWidth);
    long minY = (long) ((envelope.getMinY() - minLat) / latWidth);
    long maxY = (long) ((envelope.getMaxY() - minLat) / latWidth);
    List<Long> res = new ArrayList<>(3);
    if (minX == maxX && minY == maxY) {
      return res;
    } else if (minX == maxX && minY < maxY) {
      res.add(combineXY(minX, maxY));
      return res;
    } else if (minX < maxX && minY == maxY) {
      res.add(combineXY(minX, minY));
      return res;
    } else {
      long curX = (long) ((lng - minLng) / lngWidth);
      long curY = (long) ((lat - minLat) / latWidth);
      if (curX == minX && curY == minY) {
        res.add(combineXY(minX, maxY));
        res.add(combineXY(maxX, maxY));
        return res;
      } else if (curX == minX && curY == maxY) {
        return res;
      } else if (curX == maxX && curY == maxY) {
        res.add(combineXY(minX, maxY));
        return res;
      } else if (curX == maxX && curY == minY) {
        res.add(combineXY(minX, minY));
        res.add(combineXY(minX, maxY));
        res.add(combineXY(maxX, maxY));
        return res;
      }
    }
    return res;
  }


  @Override
  public List<Long> getRangeIndex(double minLat, double minLng, double maxLat, double maxLng) {
    long minX = (long) ((minLat + 90.d) / latWidth);
    long maxX = (long) ((maxLat + 90.d) / latWidth);
    long minY = (long) ((minLng + 180.d) / lngWidth);
    long maxY = (long) ((maxLng + 180.d) / lngWidth);
    int n = (int) ((maxX - minX + 1) * (maxY - minY + 1));
    List<Long> res = new ArrayList<>(n);
    for (long i = minX; i <= maxX; ++i) {
      for (long j = minY; j <= maxY; ++j) {
        res.add(combineXY(i, j));
      }
    }
    return res;
  }

  @Override
  public List<Long> getIntersectIndex(Geometry geom) {
    // TODO: optimize this
    return getIndex(geom.getEnvelopeInternal());
  }



  @Override
  public void getGeoBoundary(long index) {

  }


  @Override
  public List<Long> kRing(long index, int k) {
    List<Long> res = new ArrayList<>(8);
    long[] coor = this.getXY(index);
    long x = coor[0];
    long y = coor[1];
    for (long i = x - 1; i <= x + 1; i++) {
      res.add(combineXY(i, y + 1));
      res.add(combineXY(i, y - 1));
    }
    res.add(combineXY(x-1, y));
    res.add(combineXY(x+1, y));
    return res;
  }

  private long combineXY(long x, long y) {
    return x << MAX_BITS | y;
  }

  @VisibleForTesting
  public long[] getXY(long index) {
    long y = index & 0x3fffffff;
    long x = index >> MAX_BITS;
    return new long[] {x, y};
  }

  /**
   * 以四邻域或八邻域获取一个网格的邻居网格索引值。
   * @param index 中心网格索引
   * @param type 邻域类型
   */
  public List<Long> getNeighbors(long index, int type) {
    if (type == 8) {
      return this.kRing(index, 1);
    } else if (type == 4) {
      long[] coor = this.getXY(index);
      long x = coor[0];
      long y = coor[1];
      List<Long> res = new LinkedList<>();
      res.add(combineXY(x + 1, y));
      res.add(combineXY(x - 1, y));
      res.add(combineXY(x, y + 1));
      res.add(combineXY(x, y - 1));
      return res;
    }
    return null;
  }

  /**
   * 获取一个网格所对应的四叉树分区ID
   * @param index 网格索引值
   * @param level 四叉树划分层级，对应分区数量为 4 ^ level;
   * @return 分区ID，最多2*level位
   */
  public Long getPartition(long index, int level) {
    long[] coor = this.getXY(index);
    long partitionX = (long) (coor[0] / (((long) ((maxLat - minLat) / latWidth)) / Math.pow(2.0, level)));
    long partitionY = (long) (coor[1] / (((long) ((maxLng - minLng) / lngWidth)) / Math.pow(2.0, level)));
    return partitionX << level | partitionY;
  }

  public Polygon getGridPolygon(long index) {
    long[] xy = getXY(index);
    double cLng = xy[0] * lngWidth + minLng;
    double cLat = xy[1] * latWidth + minLat;
    double west = Math.round((cLng) * 1000000) / 1000000d;
    double east = Math.round((cLng + lngWidth) * 1000000) / 1000000d;
    double south = Math.round((cLat) * 1000000) / 1000000d;
    double north = Math.round((cLat + latWidth) * 1000000) / 1000000d;
    Coordinate[] vertexs = new Coordinate[5];
    vertexs[0] = new Coordinate(west, south);
    vertexs[1] = new Coordinate(east, south);
    vertexs[2] = new Coordinate(east, north);
    vertexs[3] = new Coordinate(west, north);
    vertexs[4] = new Coordinate(west, south);
    GeometryFactory factory = new GeometryFactory();
    return new Polygon(new LinearRing(new CoordinateArraySequence(vertexs), factory), null, factory);
  }
}
