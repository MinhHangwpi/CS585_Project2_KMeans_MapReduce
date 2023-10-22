import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.clustering.EuclideanDoublePoint;

public class KMAggregatePoint {

    private Integer count = 1;
    private KMPoint avgPoint = null;
    KMAggregatePoint(KMPoint avgPoint, int count) {
        this.count = count;
        this.avgPoint = avgPoint;
    }
    KMAggregatePoint (String coords) {
        String[] fields = coords.split(",");
        if (fields[0].charAt(0) == '[') {
            System.out.println("Exit 5");
            System.exit(5);
        }
        int dimensions = fields.length - 1;
//        for (int i = 0; i < dimensions; i++)
        String[] pointstr = ArrayUtils.subarray(fields, 0, dimensions);
        this.avgPoint = new KMPoint(pointstr);
        this.count = Integer.parseInt(fields[dimensions]);
    }
    KMAggregatePoint(KMAggregatePoint srcPoint) {
        this.avgPoint = srcPoint.getAvgPoint();
        this.count = srcPoint.getCount();
    }

    KMAggregatePoint(double[] xAvg, int xCount) {
        this.avgPoint = new KMPoint(xAvg);
        this.count = xCount;
    }

    public KMPoint getAvgPoint() {
        return this.avgPoint;
    }

    public int getDimension() {
        return this.avgPoint.getDimension();
    }

    public EuclideanDoublePoint getEuclidean() {
        return this.avgPoint.getEuclidean();
    }

    public int getCount() {
        return this.count;
    }

    public String getCoordsAndCountString() {
        return this.getCoordsString() + "," + this.count;
    }

    public String getCoordsString() {
        return this.avgPoint.getCoordsString();
    }

}
