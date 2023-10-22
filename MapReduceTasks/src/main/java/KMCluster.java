import org.apache.commons.math3.stat.clustering.EuclideanDoublePoint;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

public class KMCluster {
    private ArrayList<KMAggregatePoint> points = new ArrayList<>();
    private KMAggregatePoint avgPoint = null;
    // private ArrayList<String> pointsCsv = new ArrayList<String>();

    private Integer numDimensions = 0;
    // private Integer precision = 6; // number of decimal places to keep

    // Next enforce granularity on data points, and point sets allow duplicat points

    KMCluster() {
        this.numDimensions = 0;
        // this.precision = 6;
    }

/*
    public KMPoint calcCentroid () {
        int numAggPoints = this.points.size();
        if (numAggPoints > 0) {
            ArrayList<Double> pointValues = new ArrayList<>(this.numDimensions); // values of points per-dimension
            ArrayList<Double> pointSum = new ArrayList<>(this.numDimensions);
            for (int j = 0; j < this.numDimensions; j++) {
                int pointCount = 0;
                pointSum[j] = 0.0;
                for (int i = 0; i < numaggPoints; i++) {
                    KMAggregatePoint aggPoint = this.points[i];
                    double [] coords = aggPoint.getEuclidean().getPoint();
                    pointCount += aggPoint.getCount();
                    pointSum[j] += coords[j];
                }
                pointValues[j] = pointSum[j] / pointCount;
            }
            return new KMPoint(new EuclideanDoublePoint(pointValues));
        }
    }
*/
    public KMCentroid calcCentroid(Integer id) {
        return new KMCentroid(id, this.aggregatePoints().getAvgPoint());
     }

    public void addAggregatePoint(String coords) { // aggregate count is at the end of this
        KMAggregatePoint aPoint = new KMAggregatePoint(coords);
        if (this.numDimensions == 0) {
            this.numDimensions = aPoint.getDimension();
        }
        else if (this.numDimensions != aPoint.getDimension()) {
            // throw exception
            return;
        }
        int num_points = this.points.size();
        if (num_points == 0) {
            this.avgPoint = new KMAggregatePoint(aPoint);
        }
        else {
            double[] currPointsArray = this.getPointDoubleArray();
            int currWeight = this.avgPoint.getCount();
            int addingWeight = aPoint.getCount();
            double[] addingPointsArray = aPoint.getEuclidean().getPoint();
            double[] newPointsArray = new double[this.numDimensions];
            for (int i = 0; i < newPointsArray.length; i++) {
                newPointsArray[i] = ((currPointsArray[i] * currWeight)
                                     + (addingPointsArray[i] * addingWeight))
                                    / (currWeight + addingWeight);
            }
            Integer newCount = currWeight + addingWeight;
            this.avgPoint = new KMAggregatePoint(newPointsArray, newCount);
        }
        this.points.add(aPoint);
    }

    public ArrayList<KMAggregatePoint> getPoints() {
        return this.points;
    }

    private double[] getPointDoubleArray() {
        return this.avgPoint.getAvgPoint().getEuclidean().getPoint();
    }


/*
    public static KMAggregatePoint aggregatePoints() {
        String sumstr = "";
        double[] sumX = new double[this.numDimensions];
        int aggCount;

        for (KMAggregatePoint aggPoint : this.points) {
            String valstr = aggPoint.getCoordsAndCountString();
            if (valstr.length() != (this.numDimensions + 1)) {
                // throw exception
                return;
            }
            KMPoint point = new KMPoint(valstr);
            double[] pointvals = aggPoint.getEuclidean().getPoint();
            int countVal = aggPoint.getCount();
            for (int i = 0; i < this.numDimensions; i++) {
                // the aggregate point has the average of all the values,
                // so we have to multiply by the weight of the count of the points
                sumX[i] += (pointvals[i] * countVal);
            }
            // and then we'll divide by the total number of points in the cluster
            aggCount += aggPoint.getCount();
        }
        double [] pointAvg;
        for (i = 0; i < this.numDimensions; i++) {
            pointAvg[i] = sumX[i] / aggCount;
        }
        return new KMAggregatePoint(new KMPoint(new EuclideanDoublePoint(pointAvg)), aggCount);
    }
*/
    public KMAggregatePoint aggregatePoints() {
        String sumstr = "";
        double[] sumX = new double[this.numDimensions];
        int aggCount = 0;

        for (KMAggregatePoint aggPoint : this.points) {
            String valstr = aggPoint.getCoordsAndCountString();
            if (valstr.length() != (this.numDimensions + 1)) {
                // throw exception
                return this.avgPoint;
            }
            KMPoint point = new KMPoint(valstr);
            double[] pointvals = aggPoint.getEuclidean().getPoint();
            int countVal = aggPoint.getCount();
            for (int i = 0; i < this.numDimensions; i++) {
                // the aggregate point has the average of all the values,
                // so we have to multiply by the weight of the count of the points
                sumX[i] += (pointvals[i] * countVal);
            }
            // and then we'll divide by the total number of points in the cluster
            aggCount += aggPoint.getCount();
        }
        double [] pointAvg = new double[this.numDimensions];
        for (int i = 0; i < this.numDimensions; i++) {
            pointAvg[i] = sumX[i] / aggCount;
        }
        this.avgPoint = new KMAggregatePoint(new KMPoint
                (new EuclideanDoublePoint(pointAvg)), aggCount);
        return this.avgPoint;
    }
}
