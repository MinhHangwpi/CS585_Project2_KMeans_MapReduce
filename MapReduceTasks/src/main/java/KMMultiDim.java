import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
// import java.util.Arrays;
import java.util.Objects;

import org.apache.commons.math3.stat.clustering.EuclideanDoublePoint;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static org.apache.commons.io.FileUtils.copyFile;

/*
public boolean checkForType(Object candidate, Class<?> type){
        return type.isInstance(candidate);
}
*/


public class KMMultiDim {

//    public static EuclideanDoublePoint coordsStringToEuclideanDoublePoint(String coords) {
//        String[] fields = coords.split(",");
//        Double[] seedCoords = new Double[fields.length];
//        for (int i = 0; i < fields.length; i++) {
//            seedCoords[i] =Double.parseDouble(fields[i]);
//        }
//        return new EuclideanDoublePoint(seedCoords);
//
//    }

    // private static int dimension = 0; // always enforce same dimension
    // private static String KMsubtask = "a";
    // private static String KMsubtask = "b";
    // private static String KMsubtask = "c";
    // private static String KMsubtask = "d";
    // private static String KMsubtask = "ei";
    private static String KMsubtask = "eii";

    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text centroidKey = new Text();
        private final Text pointValue = new Text();
        // private ArrayList<String> k_strings = new ArrayList<>();
        private final ArrayList<KMCentroid> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            // populating the seeds array list with values from file in distributed cache
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);

            // open the stream
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);

            // wrap it into a BufferedReader object which is easy to read a record
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String line;

            while ((line = reader.readLine()) != null) {
           //     String[] fields = line.split(",");
           //     String subList[] = Arrays.copyOfRange(fields, 1, fields.length());
                centroids.add(new KMCentroid(line));
            //Integer(Integer.parseInt(fields[0])),
                    //    new KMAggregatePoint(new KMPoint(subList), 1)));
             }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // each line is a point
            KMPoint point = new KMPoint(value.toString());

            KMCentroid closestCentroid = point.findClosestCentroid(centroids);

            Integer id = closestCentroid.getId();
            centroidKey.set(id.toString());
 /*           pointValue.set(centroidFlag + "," + closestCentroid.getDimension() + ","
                    + closestCentroid.getCoordsAndCountString());
            context.write(centroidKey, pointValue);
*/

/*
            pointValue.set(closestCentroid.getDimension() + ","
                    + closestCentroid.getCoords() + "," + closestCentroid.getDimension() + ","
                    + value + ",1");
*/
            pointValue.set(value + ",1");
            context.write(centroidKey, pointValue);  // output the closest centroid and each point # 5000 lines
        }


     }


    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text partialAvg = new Text();

        @Override
        protected void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // double[] sumX;
            KMCluster cluster = new KMCluster();

            for (Text value : values) {
                String valstr = value.toString();
                if (valstr.charAt(0) == '[') {
                    System.out.println("Exit 1");
                    System.exit(1);
                }
                cluster.addAggregatePoint(valstr);
/*
                String[] fields = value.split(",");
                int flag = Integer(Integer.parseInt(fields[1]));
                if (flag == pointFlag) {
                    cluster.addAggregatePoint(value);
                }
                else {
                    context.write(key, value);
                }
*/
            }
            KMAggregatePoint newPoint = cluster.aggregatePoints();
            // get x array and count
//            KMPoint point = newPoint.getAvgPoint();
//            double[] pointvals = newPoint.getAvgPoint().getEuclidean().getPoint();
//            for (int i = 0; i < dimension; i++) {
//                sumX[i] += pointvals[i];
//            }
            String cacs = newPoint.getCoordsAndCountString();
            if (cacs.charAt(0) == '[') {
                System.out.println("Exit 2");
                System.exit(2);
            }
            partialAvg.set(cacs);
            context.write(key, partialAvg);
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * The reduce function receives a key-value pair where the key is a centroid and the values are the list of points that are closest to that centroid.
         */
        private final Text newCentroid = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // double[] sumX;
            KMCluster cluster = new KMCluster();
            for (Text value : values) {
                String valstr = value.toString();
                if (valstr.charAt(0) == '[') {
                    System.out.println("Exit 3");
                    System.exit(3);
                }
               cluster.addAggregatePoint(valstr);
            }
            Integer id = Integer.parseInt(key.toString());
            KMCentroid centroid = cluster.calcCentroid(id);
            String cos = centroid.getCoordsString();
            if (cos.charAt(0) == '[') {
                System.out.println("Exit 4");
                System.exit(4);
            }

            newCentroid.set(cos);

/*
            boolean hasConverged = checkConvergence(
                    outputPathBase + "/iteration_" + i,
                    outputPathBase + "/iteration_" + (i + 1),
                    previousCentroids,
                    currentCentroids,
                    threshold
            ); // should this be /iteration_ i + 1
            if (hasConverged) {
                System.out.println("KMeans Clustering converged after " + (i + 1) + " iterations.");
                break;
            }
*/
            context.write(key, newCentroid);
        }
    }

    public static boolean checkConvergence(String previousCentroidBasePath, String currentCentroidBasePath,
                                           // ArrayList<KMCentroid> previousCentroids,
                                           // ArrayList<KMCentroid> currentCentroids,
                                           double threshold) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path previousCentroidPath = new Path(previousCentroidBasePath + "/part-r-00000");
        Path currentCentroidPath = new Path(currentCentroidBasePath + "/part-r-00000");

        ArrayList<KMCentroid> previousCentroids = new ArrayList<>();
        ArrayList<KMCentroid> currentCentroids = new ArrayList<>();

        // Read the centroids from the previous iteration
        BufferedReader reader1 = new BufferedReader((new InputStreamReader(fs.open(previousCentroidPath), "UTF-8")));
        String line1;

        // ArrayList<KMCentroid> previousCentroids = new ArrayList<>();
        // int id = 1;
        while ((line1 = reader1.readLine()) != null) {
 /*           System.out.println("Processing line: " + line1);

            String[] centroidCoordinates = line1.split(",");
            int centroidX = Integer.parseInt(centroidCoordinates[0]);
            int centroidY = Integer.parseInt(centroidCoordinates[1]);
*/

            previousCentroids.add(new KMCentroid(line1));
 //                   id, KMAggregatePoint.coordsStringToKMAggregatePoint(line1)));
        }
        reader1.close();

        // Read the centroids from the current iteration
        BufferedReader reader2 = new BufferedReader((new InputStreamReader(fs.open(currentCentroidPath), "UTF-8")));
        String line2;
        // ArrayList<KMCentroid> currentCentroids = new ArrayList<>();
        while ((line2 = reader2.readLine()) != null) {
            // System.out.println("Processing line: " + line2);
            currentCentroids.add(new KMCentroid(line2));
        }
        reader2.close();

        // O(k*k) complexity but hopefully k is small
        boolean hasConverged = false;
        for (KMCentroid currentCentroid : currentCentroids) {
            // double minDistance = Double.MAX_VALUE;
            EuclideanDoublePoint currPoint = currentCentroid.getPoint().getEuclidean();
            for (KMCentroid previousCentroid : previousCentroids) {
                if (Objects.equals(currentCentroid.getId(), previousCentroid.getId())) {
                    EuclideanDoublePoint prevPoint = previousCentroid.getPoint().getEuclidean();
                    double distance = prevPoint.distanceFrom(currPoint);
                    boolean converged = (distance <= threshold);
                    if (converged) {
                        hasConverged = true;
                    }
                    currentCentroid.setConverged(converged);
                }
            }
        }
        return hasConverged;
    }

    public static void main(String[] args) throws Exception {

        // Start time
        long startTime = System.currentTimeMillis();

/*
        if (args.length != 3) {
            System.err.println("Usage: KMeansDriver <input all points> <input path seeds> <output path>");
            System.exit(1);
        }
*/

        // defaults
        boolean stopIfConverged = false; // stop early if converged, but does not continue beyond R rounds if not
        boolean outputFinalPoints = false; // output what the final points are along with their centroids
        int R = 7; // number of rounds to run
        double threshold = 0.01; // convergence threshold
        boolean optimize = false; //optimize with combiner
        boolean indicateConvergence = false;
        // override defaults for subtasks
        if (KMsubtask == "a") {
            R = 1;;
        }
        else if (KMsubtask == "b") {
            // use all defaults
        }
        else if (KMsubtask == "c") {
            stopIfConverged = true;
        }
        else if (KMsubtask == "d") {
            stopIfConverged = true;
            optimize = true;
        }
        else if (KMsubtask == "ei") {
            stopIfConverged = true;
            optimize = true;
            indicateConvergence = true;
        }
        else if (KMsubtask == "eii") {
            stopIfConverged = true;
            optimize = true;
            indicateConvergence = true;
            outputFinalPoints = true;
        }

        boolean hasConverged = false;

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        FileSystem fs = FileSystem.get(conf);

        String inputPath = "/user/cs4433/project2/input/Task3BYOD_cab_normMM_10.csv";
        String seedsPath = "/user/cs4433/project2/input/Task3BYOD_centroids.csv";
        String outputPathBase = "/user/cs4433/project2/cab_output";
        // if (fs.exists(new Path(outputPathBase))) {
           // fs.delete(new Path(outputPathBase), true);
        // }

        int i = 0;
        int convergedIterations = 0;

        while (i < R) {
            Job job = Job.getInstance(conf, "KMeans Clustering - Iteration " + (i + 1));

            // Add the seeds (centroids) file to the cache for this job
            job.addCacheFile(new URI(i == 0 ? seedsPath : (outputPathBase + "/iteration_" + i + "/part-r-00000")));

            job.setJarByClass(KMMultiDim.class);

            // Set input path: For the first iteration, use the initial input. For subsequent iterations, use the output of the previous iteration
            FileInputFormat.addInputPath(job, new Path(inputPath));
            // Set output path for this iteration
            Path outputPath = new Path(outputPathBase + "/iteration_" + (i + 1));
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setMapperClass(KMeansMapper.class);
            if (optimize && !outputFinalPoints) {
                job.setCombinerClass(KMeansCombiner.class);
            }
            job.setReducerClass(KMeansReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Running the map reduce job for the iteration
            boolean success = job.waitForCompletion(true);
            if (!success) {
                System.out.println("KMeans Clustering failed on iteration " + (i + 1));
                System.exit(1);
            }

            //Check for convergence by comparing the centroids from this iteration to the previous iteration. Break if it has converged
            /*
            ArrayList<KMCentroid> previousCentroids = new ArrayList<>();
            ArrayList<KMCentroid> currentCentroids = new ArrayList<>();
            */


            if (i > 0 && !hasConverged) {
                hasConverged = checkConvergence(
                        outputPathBase + "/iteration_" + i,
                        outputPathBase + "/iteration_" + (i + 1),
                        /* previousCentroids,
                        currentCentroids, */
                        threshold
                ); // should this be /iteration_ i + 1
                if (hasConverged) {
                    System.out.println("KMeans Clustering converged after " + (i + 1) + " iterations.");
                    convergedIterations = i + 1;
                    if (stopIfConverged) {
                        break;
                    }
                }
            }
            i++;
        }

        /*Path destPath = new Path(outputPathBase + "/final_centroids");
        if (fs.exists(destPath)) {
            fs.delete(destPath, true);
        }
*/
        // Files.copy(new Path(outputPathBase + "/iteration_" + (i + 1) + "/SUCCESS"), new Path(outputPathBase + "/final_centroids/SUCCESS"));
        // Files.copy(new Path(outputPathBase + "/iteration_" + (i + 1) + "/part-r-00000"), new Path(outputPathBase + "/final_centroids/part-r-00000"));
        if (!hasConverged) {
            System.out.println("KMeans Clustering has not converged after " + i + " iterations.");
        }
        else {
            System.out.println("KMeans Clustering converged after " + convergedIterations + " iterations.");
        }
        // Essentially copying latest iteration to a specific filename
  /*      Job clusterMappingJob = Job.getInstance(conf, "Mapping Data to Clusters");

        clusterMappingJob.addCacheFile(new URI(outputPathBase + "/iteration_" + (i + 1) + "/part-r-00000")); // final centroids as cache

        clusterMappingJob.setJarByClass(KMMultiDim.class);

        MultipleInputs.addInputPath(clusterMappingJob, new URI(outputPathBase + "/iteration_" + (i + 1) + "/part-r-00000"),
                TextInputFormat.class, FinalCentroidsMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath),
                TextInputFormat.class, KMeansMapper.class);

        FileInputFormat.addInputPath(clusterMappingJob, new Path(inputPath));

        Path finalOutputPath = new Path(outputPathBase + "/final_clusters");
        if (fs.exists(finalOutputPath)) {
            fs.delete(finalOutputPath, true);
        }
        FileOutputFormat.setOutputPath(clusterMappingJob, finalOutputPath);

        clusterMappingJob.setMapperClass(KMeansMapper.class);
        // clusterMappingJob.setReducerClass(Reducer.class);  // use default IdentityReducer

        clusterMappingJob.setOutputKeyClass(Text.class);
        clusterMappingJob.setOutputValueClass(Text.class);

        clusterMappingJob.waitForCompletion(true);
*/
        if (outputFinalPoints) {
            // Mapping data points to final clusters
            Job clusterMappingJob = Job.getInstance(conf, "Mapping Data to Clusters");

            clusterMappingJob.addCacheFile(new URI(outputPathBase + "/iteration_" + (i + 1) + "/part-r-00000")); // final centroids as cache

            clusterMappingJob.setJarByClass(KMMultiDim.class);

            FileInputFormat.addInputPath(clusterMappingJob, new Path(inputPath));

            Path finalOutputPath = new Path(outputPathBase + "/final_clusters");
            if (fs.exists(finalOutputPath)) {
                fs.delete(finalOutputPath, true);
            }
            FileOutputFormat.setOutputPath(clusterMappingJob, finalOutputPath);

            clusterMappingJob.setMapperClass(KMeansMapper.class);
            // clusterMappingJob.setReducerClass(Reducer.class);  // use default IdentityReducer

            clusterMappingJob.setOutputKeyClass(Text.class);
            clusterMappingJob.setOutputValueClass(Text.class);

            clusterMappingJob.waitForCompletion(true);
        }

        // End time and calculate total time
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Total runtime: " + totalTime + " milliseconds");
    }
}
