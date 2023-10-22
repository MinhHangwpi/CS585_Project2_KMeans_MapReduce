import org.apache.commons.math3.stat.clustering.EuclideanDoublePoint;

import java.util.ArrayList;
// import java.util.Vector;
// import org.apache.commons.math3.ml.distance;

public class KMPoint {
    private String coordsString = "";
    private EuclideanDoublePoint x;

    KMPoint(String coordsString) {
        this.coordsString = coordsString;
        this.x = coordsStringToEuclideanDoublePoint(coordsString);
    }

    KMPoint(String[] coordsStringList) {
        this.coordsString = String.join(",", coordsStringList);
        this.x = coordsStringListToEuclideanDoublePoint(coordsStringList);
    }

    KMPoint(EuclideanDoublePoint point) {
        this.coordsString = strEuclidean(point);
        this.x = point;
    }

    KMPoint(double[] point) {
        EuclideanDoublePoint newPoint = new EuclideanDoublePoint(point);
        this.coordsString = strEuclidean(newPoint);
        // System.out.println("string from point is " + this.coordsString);
        this.x = newPoint;
    }

    public static EuclideanDoublePoint coordsStringToEuclideanDoublePoint(String coords) {
        String[] fields = coords.split(",");
        return coordsStringListToEuclideanDoublePoint(fields);
     }
    public static EuclideanDoublePoint coordsStringListToEuclideanDoublePoint(String[] coords) {
        double[] seedCoords = new double[coords.length];
        for (int i = 0; i < seedCoords.length; i++) {
            seedCoords[i] = Double.parseDouble(coords[i]);
        }
        return new EuclideanDoublePoint(seedCoords);

    }


    public EuclideanDoublePoint getEuclidean() {
        return this.x;
    }

    public String getCoordsString() {
        return this.coordsString;
    }

    public double distanceFrom(KMPoint from) {
        return this.x.distanceFrom(from.getEuclidean());
    }

    public double[] getPoint(KMPoint point) {
        return getEuclidean().getPoint();
    }

    public static KMPoint findClosestPoint(KMPoint srcPoint,
                                           ArrayList<KMPoint> pointList) {
        KMPoint currentClosestPoint = null;
        double currentMinDist = Double.MAX_VALUE;

        for (KMPoint point : pointList) {
            double distance = point.distanceFrom(srcPoint);
            if (distance < currentMinDist) {
                currentMinDist = distance;
                currentClosestPoint = point;
            }
        }
        return currentClosestPoint;
    }
    public KMCentroid findClosestCentroid(ArrayList<KMCentroid> centroidList) {
        KMCentroid currentClosestCentroid = null;
        double currentMinDist = Double.MAX_VALUE;

        for (KMCentroid point : centroidList) {
            double distance = point.getPoint().distanceFrom(this);
            if (distance < currentMinDist) {
                currentMinDist = distance;
                currentClosestCentroid = point;
            }
        }
        return currentClosestCentroid;
    }


    public static String strEuclidean(EuclideanDoublePoint point) {
        String[] points = point.toString().split(", ");
        String pointsStr = String.join(",", points);
        if (pointsStr.length() >= 2) {
            String coords = pointsStr.substring(1, pointsStr.length() - 1);
            return coords;
        }
        return "";
    }

    public int getDimension() {
        return this.x.getPoint().length;
    }
}
