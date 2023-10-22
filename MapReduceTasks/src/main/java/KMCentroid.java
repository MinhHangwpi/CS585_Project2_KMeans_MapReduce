import java.util.Arrays;

public class KMCentroid {

    private Integer C_Id = 0;
    private KMPoint C_point;
    private boolean C_converged = false;

    KMCentroid(Integer id, KMPoint point) {
        C_Id = id;
        C_point = point;
        C_converged = false;
    }

    KMCentroid(String defString) {
        String[] fields = defString.split(",");
        this.C_Id = Integer.parseInt(fields[0]);
        String subList[] = Arrays.copyOfRange(fields, 1, fields.length);
        String coordsString = String.join(",", subList);
        this.C_point = new KMPoint(coordsString);
    }


    public Integer getId() {
        return C_Id;
    }

    public KMPoint getPoint() {
        return C_point;
    }

    // public int getDimension() {
        //return C_point.getDimension();
    //}

    // public String getCoordsAndCountString() {
        //return this.C_point.getCoordsAndCountString();
    //}
    public String getCoordsString() {
        return getPoint().getCoordsString();
    }

    public void setConverged(boolean converged) {
        C_converged = converged;
    }

    public boolean hasConverged(){
        return C_converged;
    }
}
