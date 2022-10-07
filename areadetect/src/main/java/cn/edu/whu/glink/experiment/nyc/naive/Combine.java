package cn.edu.whu.glink.experiment.nyc.naive;


import cn.edu.whu.glink.areadetect.feature.AreaID;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.index.TRTreeIndex;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.distance.DistanceUtils;

import java.util.List;



public class Combine implements AggregateFunction<Tuple2<AreaID, List<DetectUnit>>, TRTreeIndex<Geometry>, List<Geometry> > {

  private double dist;

  public Combine(double dist) {
    this.dist = dist;
  }

  @Override
  public TRTreeIndex<Geometry> createAccumulator() {
    return new TRTreeIndex<>();
  }

  @Override
  public TRTreeIndex<Geometry> add(Tuple2<AreaID, List<DetectUnit>> value, TRTreeIndex<Geometry> tree) {
    Geometry geom = null;
    for(DetectUnit unit : value.f1) {
      if(geom == null) {
        geom = unit.getPolygon();
      } else {
        geom = geom.union(unit.getPolygon());
      }
    }
    assert geom != null;
    // get Envelope and query
    List<Geometry> list = tree.query(geom.buffer(DistanceUtils.KM_TO_DEG * dist));
    for(Geometry tocombine : list) {
      tree.remove(tocombine);
      geom.union(tocombine);
    }
    tree.insert(geom);
    return tree;
  }

  @Override
  public List<Geometry> getResult(TRTreeIndex<Geometry> accumulator) {
    return accumulator.query(new Envelope(-180, 180, -90, 90));
  }

  @Override
  public TRTreeIndex<Geometry> merge(TRTreeIndex<Geometry> a, TRTreeIndex<Geometry> b) {
    return null;
  }
}
