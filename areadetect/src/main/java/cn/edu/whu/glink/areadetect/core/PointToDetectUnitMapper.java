package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.datatypes.DetectUnit;
import cn.edu.whu.glink.areadetect.datatypes.PickDropPoint;
import cn.edu.whu.glink.areadetect.index.GeographicalGridIndex;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Haocheng Wang
 * Created on 2022/10/8
 */
public class PointToDetectUnitMapper implements MapFunction<PickDropPoint, DetectUnit> {

    private GeographicalGridIndex gridIndex;

    public PointToDetectUnitMapper(double rasterSize) {
      gridIndex = new GeographicalGridIndex(rasterSize);
    }

    @Override
    public DetectUnit map(PickDropPoint value) throws Exception {
      double lng = value.getLng();
      double lat = value.getLat();
      long time = value.getTime();
      boolean isPickUp = value.isPickUp();

      return (new DetectUnit(gridIndex.getIndex(lng, lat), time, 1));
    }
  }


