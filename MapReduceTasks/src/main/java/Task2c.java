import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task2c {

    public static class Point<X, Y> {
        public final X x;
        public final Y y;

        public Point(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }

    private static Map<String, Point<Integer, Integer>> centroids = new HashMap<>();

    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text centroidKey = new Text();
        private Text pointValue = new Text();
        private ArrayList<Point<Integer, Integer>> seeds = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);

            // open the stream
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);

            // wrap it into a BufferedReader object which is easy to read a record
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String line;

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                Point<Integer, Integer> seed = new Point<>(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]));
                seeds.add(seed);
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] point = value.toString().split(",");

            String closestCentroid = findClosestCentroid(point);

            centroidKey.set(closestCentroid);
            pointValue.set(value);

            context.write(centroidKey, pointValue);
        }

        private String findClosestCentroid(String[] point) {
            int x = Integer.parseInt(point[0]);
            int y = Integer.parseInt(point[1]);

            double currentMinDist = Double.MAX_VALUE;
            Point<Integer, Integer> currentCentroid = null;

            for (Point<Integer, Integer> seed : seeds) {
                double distance = Math.sqrt(Math.pow(x - seed.x, 2) + Math.pow(y - seed.y, 2));
                if (distance < currentMinDist) {
                    currentMinDist = distance;
                    currentCentroid = seed;
                }
            }
            return currentCentroid.x + "," + currentCentroid.y;
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {

        private Text newCentroid = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumX = 0;
            int sumY = 0;
            int count = 0;

            for (Text value : values) {
                String[] point = value.toString().split(",");
                sumX += Integer.parseInt(point[0].trim());
                sumY += Integer.parseInt(point[1].trim());
                count++;
            }

            int centroidX = sumX / count;
            int centroidY = sumY / count;
            newCentroid.set(centroidX + "," + centroidY);

            context.write(key, newCentroid);
        }
    }

    public static boolean checkConvergence(Map<String, Point<Integer, Integer>> previousCentroids, String outputPath, double threshold) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path previousCentroidsPath = new Path(outputPath + "/part-r-00000"); // Adjust the path as needed

        // Read the centroids from the previous iteration
        BufferedReader reader = new BufferedReader((new InputStreamReader(fs.open(previousCentroidsPath), "UTF-8")));
        String line;
        Map<String, Point<Integer, Integer>> currentCentroids = new HashMap<>();

        while ((line = reader.readLine()) != null){
            String[] fields = line.split("\t");
            String clusterId = fields[0];
            String[] centroidCoordinates = fields[1].split(",");
            int centroidX = Integer.parseInt(centroidCoordinates[0]);
            int centroidY = Integer.parseInt(centroidCoordinates[1]);

            currentCentroids.put(clusterId, new Point<>(centroidX, centroidY));
        }

        // Compare centroids between the current and previous iterations
        for (String clusterId : currentCentroids.keySet()){
            Point<Integer, Integer> currentCentroid = currentCentroids.get(clusterId);
            Point<Integer, Integer> previousCentroid = previousCentroids.get(clusterId);

            double distance = Math.sqrt(Math.pow(currentCentroid.x - previousCentroid.x, 2) + Math.pow(currentCentroid.y - previousCentroid.y, 2));

            // adjust the convergence threshold accordingly
            if (distance > threshold){
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: KMeansDriver <input all points> <input path seeds> <output path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String inputPath = args[0];
        String seedsPath = args[1];
        String outputPathBase = args[2];

        // Create a global variable for tracking centroids
//        private static Map<String, Point<Integer, Integer>> centroids = new HashMap<>();

        final int R = 10;  // Number of iterations
        for (int i = 0; i < R; i++) {
            Job job = Job.getInstance(conf, "KMeans Clustering - Iteration " + (i + 1));

            // Add the seeds (centroids) file to the cache for this job
            job.addCacheFile(new URI(seedsPath));

            job.setJarByClass(Task2c.class);

            // Set input path: For the first iteration, use the initial input. For subsequent iterations, use the output of the previous iteration
            FileInputFormat.addInputPath(job, new Path(i == 0 ? inputPath : (outputPathBase + "/iteration_" + i)));

            // Set output path for this iteration
            Path outputPath = new Path(outputPathBase + "/iteration_" + (i + 1));
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            boolean success = job.waitForCompletion(true);
            if (!success) {
                System.out.println("KMeans Clustering failed on iteration " + (i + 1));
                System.exit(1);
            }

            //Check for convergence by comparing the centroids from this iteration to the previous iteration. Break if it has converged

            if (i > 0) {
                boolean hasConverged = checkConvergence( centroids, outputPathBase + "/iteration_" + i, 0.01);
                if (hasConverged){
                    System.out.println("KMeans Clustering converged after "+ (i + 1) + " iterations.");
                    break;
                }
            }
        }
        System.out.println("KMeans Clustering completed after " + R + " iterations.");
    }
}