package cn.edu.whu.glink.areadetect.datatypes;

/**
 * @author Haocheng Wang
 * Created on 2022/10/8
 */
public class PickDropPoint {
  double lng;
  double lat;
  long time;
  boolean isPickUp;
  String carID;

  public PickDropPoint(double lng, double lat, long time, boolean isPickUp, String carID) {
    this.lng = lng;
    this.lat = lat;
    this.time = time;
    this.isPickUp = isPickUp;
    this.carID = carID;
  }

  public double getLng() {
    return lng;
  }

  public void setLng(double lng) {
    this.lng = lng;
  }

  public double getLat() {
    return lat;
  }

  public void setLat(double lat) {
    this.lat = lat;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public boolean isPickUp() {
    return isPickUp;
  }

  public void setPickUp(boolean pickUp) {
    isPickUp = pickUp;
  }

  public String getCarID() {
    return carID;
  }

  public void setCarID(String carID) {
    this.carID = carID;
  }
}
