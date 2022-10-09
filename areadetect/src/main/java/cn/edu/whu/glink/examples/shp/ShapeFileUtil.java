package cn.edu.whu.glink.examples.shp;

import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

public class ShapeFileUtil {

    public static SimpleFeatureType getSft() {
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("head-areas");
        builder.setSRS( "EPSG:4326" );
        builder.add( "areaId", String.class );
        builder.add( "date", String.class );
        builder.add( "area", Double.class );
        builder.add( "heat", Double.class );
        builder.add( "the_geom", MultiPolygon.class );
        return builder.buildFeatureType();
    }

    /**
     * 生成shape文件
     *
     * @param shpPath  生成shape文件路径（包含文件名称）
     * @param encode   编码
     * @param attrKeys 属性key集合
     * @param data     图幅和属性集合
     */
    public static void write2Shape(String shpPath, String encode, SimpleFeatureType sft, List<String> attrKeys, List<Map<String, Object>> data) {
        try {
            if (data == null || data.size() == 0) {
                return;
            }
            //创建shape文件对象
            File file = new File(shpPath);
            Map<String, Serializable> params = new HashMap<>();
            params.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());
            ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);

            ds.createSchema(sft);
            //设置编码
            Charset charset = Charset.forName(encode);
            ds.setCharset(charset);
            //设置Writer
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(ds.getTypeNames()[0], Transaction.AUTO_COMMIT);
            //写入文件信息
            for (int i = 0; i < data.size(); i++) {
                SimpleFeature feature = writer.next();
                Map<String, Object> row = data.get(i);
                for (String key : row.keySet()) {
                    feature.setAttribute(key, row.get(key));
                }
            }
            writer.write();
            writer.close();
            ds.dispose();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Geometry toMultiPolygon(Geometry geom, GeometryFactory factory) {
        if (geom instanceof Polygon) {
            Polygon[] arr = new Polygon[1];
            arr[0] = (Polygon) geom;
            return factory.createMultiPolygon(arr);
        } else {
            return geom;
        }
    }


    public static void createShp(String file, String outputFolderPath) throws IOException, ParseException, java.text.ParseException {
        SimpleDateFormat fromSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        SimpleDateFormat toSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        BufferedReader br = new BufferedReader(new FileReader(file));
        WKTReader wktReader = new WKTReader();
        String line;
        List<String> list = Arrays.asList("areaId", "date", "area", "heat", "geometry");
        List<Map<String, Object>> data = new LinkedList<>();
        SimpleFeatureType simpleFeatureType = getSft();
        GeometryFactory factory = new GeometryFactory();
        while ((line = br.readLine()) != null) {
            HashMap<String, Object> map = new HashMap<>();
            String[] tokens = line.split("\\|");
            String wkt = tokens[0];
            Geometry geom = wktReader.read(wkt);
            geom = toMultiPolygon(geom, factory);
            map.put("areaId", tokens[1]);
            map.put("date", toSdf.format(fromSdf.parse(tokens[2])));
            map.put("area", Double.parseDouble(tokens[4]));
            map.put("heat", Double.parseDouble(tokens[3]));
            map.put("the_geom", geom);
            data.add(map);
        }
        String path = outputFolderPath + "\\"+ file +"\\";
        new File(path).mkdirs();
        write2Shape( path + "result.shp", "UTF-8", simpleFeatureType, list, data);
    }
}