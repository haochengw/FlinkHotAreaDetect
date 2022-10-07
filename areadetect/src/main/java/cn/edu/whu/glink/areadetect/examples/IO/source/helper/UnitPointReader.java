package cn.edu.whu.glink.areadetect.examples.IO.source.helper;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class UnitPointReader {
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  public static HashMap<Long, Point> read(String stationFilePath) {
    HashMap<Long, Point> res = new HashMap<>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(stationFilePath));
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split(",");
        Long id = Long.parseLong(tokens[0]);
        double lng = Double.parseDouble(tokens[2]);
        double lat = Double.parseDouble(tokens[3]);
        Point point = new Point(new CoordinateArraySequence(new Coordinate[]{new Coordinate(lng, lat)}), GEOMETRY_FACTORY);
        res.put(id, point);
      }
      br.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
    return res;
  }
}
