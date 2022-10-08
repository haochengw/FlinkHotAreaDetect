package cn.edu.whu.glink.areadetect.index;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.List;

/**
 * @author Yu Liebing
 */
public abstract class GridIndex implements Serializable {

  /**
   * Resolution of grids.
   * */
  protected int res;

  public abstract int getRes();

  public abstract long getIndex(double lng, double lat);

  public abstract List<Long> getIndex(Envelope envelope);

  public abstract List<Long> getIndex(Geometry geom);

  public abstract List<Long> getIndex(double lng, double lat, double distance);

  /**
   * Use for pair range join for analysis applications like DBSCAN.
   * */
  public abstract List<Long> getIndex(double lng, double lat, double distance, boolean reduce);


  @Deprecated
  public abstract List<Long> getRangeIndex(double minLat, double minLng, double maxLat, double maxLng);

  /**
   * Get the indexes intersecting with the boundary of the input geometry.
   * @param geom The geometry to get a boundary.
   * @return Intersected indexes.
   */
  public abstract List<Long> getIntersectIndex(Geometry geom);


  /**
   * Get the boundary of the area indicated by the input index.
   * @param index The boundary.
   */
  public abstract void getGeoBoundary(long index);

  /**
   * Get a index set of the k-th layer ring centered on the current index.
   * K-ring 0 is defined as the origin index, k-ring 1 is defined as k-ring 0 and all neighboring indices, and so on.
   * @param index Current index value.
   * @param k Distance k.
   * @return A indexes set of the k-th layer ring centered on the current index.
   */
  public abstract List<Long> kRing(long index, int k);
}
