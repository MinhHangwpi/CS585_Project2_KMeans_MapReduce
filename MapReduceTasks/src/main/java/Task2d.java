import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task2d {
    public static class Point<X, Y> {
        public final X x;
        public final Y y;

        public Point(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }

    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text centroidKey = new Text();
        private Text pointValue = new Text();
        private ArrayList<Point<Integer, Integer>> seeds = new ArrayList<>();

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
                String[] fields = line.split(",");
                Point<Integer, Integer> seed = new Point<>(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]));
                seeds.add(seed);
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] point = value.toString().split(","); // each line is a point (x, y)

            String closestCentroid = findClosestCentroid(point);

            centroidKey.set(closestCentroid);
            pointValue.set(value);

            context.write(centroidKey, pointValue);  // output the closest centroid and each point # 5000 lines
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


    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
        private Text partialSum = new Text();

        @Override
        protected void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumX = 0;
            int sumY = 0;
            int count = 0;

            for (Text value: values){
                String[] point = value.toString().split(",");
                sumX += Integer.parseInt(point[0].trim());
                sumY += Integer.parseInt(point[1].trim());
                count++;
            }

            // Write partial sum and count, separated by a comma
            partialSum.set(sumX + "," + sumY + "," + count);
            context.write(key, partialSum);
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, NullWritable> {

        /**
         * The reduce function receives a key-value pair where the key is a centroid and the values are the list of points that are closest to that centroid.
         */
        private Text newCentroid = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalSumX = 0;
            int totalSumY = 0;
            int totalCount = 0;

            for (Text value : values) {
                String[] aggregatedData = value.toString().split(",");
                totalSumX += Integer.parseInt(aggregatedData[0].trim());
                totalSumY += Integer.parseInt(aggregatedData[1].trim());
                totalCount += Integer.parseInt(aggregatedData[2].trim());
            }

            int centroidX = totalSumX / totalCount;
            int centroidY = totalSumY / totalCount;
            newCentroid.set(centroidX + "," + centroidY);

            context.write(newCentroid, NullWritable.get());
        }
    }

    public static boolean checkConvergence(String previousCentroidBasePath, String currentCentroidBasePath, double threshold) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path previousCentroidPath = new Path(previousCentroidBasePath + "/part-r-00000");
        Path currentCentroidPath = new Path(currentCentroidBasePath + "/part-r-00000");

        // Read the centroids from the previous iteration
        BufferedReader reader1 = new BufferedReader((new InputStreamReader(fs.open(previousCentroidPath), "UTF-8")));
        String line1;
        ArrayList<Point<Integer, Integer>> previousCentroids = new ArrayList<>();
        while ((line1 = reader1.readLine()) != null) {
            System.out.println("Processing line: " + line1);
            String[] centroidCoordinates = line1.split(",");
            int centroidX = Integer.parseInt(centroidCoordinates[0]);
            int centroidY = Integer.parseInt(centroidCoordinates[1]);

            previousCentroids.add(new Point<>(centroidX, centroidY));
        }
        reader1.close();

        // Read the centroids from the current iteration
        BufferedReader reader2 = new BufferedReader((new InputStreamReader(fs.open(currentCentroidPath), "UTF-8")));
        String line2;
        ArrayList<Point<Integer, Integer>> currentCentroids = new ArrayList<>();
        while ((line2 = reader2.readLine()) != null) {
            System.out.println("Processing line: " + line2);
            String[] centroidCoordinates = line2.split(",");
            int centroidX = Integer.parseInt(centroidCoordinates[0]);
            int centroidY = Integer.parseInt(centroidCoordinates[1]);

            currentCentroids.add(new Point<>(centroidX, centroidY));
        }
        reader2.close();

        // O(k*k) complexity but hopefully k is small
        for (Point<Integer, Integer> currentCentroid : currentCentroids) {
            double minDistance = Double.MAX_VALUE;
            for (Point<Integer, Integer> previousCentroid : previousCentroids) {
                double distance = Math.sqrt(Math.pow(currentCentroid.x - previousCentroid.x, 2) + Math.pow(currentCentroid.y - previousCentroid.y, 2));
                if (distance < minDistance) {
                    minDistance = distance;
                }
            }
            if (minDistance > threshold) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) throws Exception {

        // Start time
        long startTime = System.currentTimeMillis();

        if (args.length != 3) {
            System.err.println("Usage: KMeansDriver <input all points> <input path seeds> <output path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String inputPath = args[0];
        String seedsPath = args[1];
        String outputPathBase = args[2]; // e.g. task_c

        boolean hasConverged = false;
        int i = 0;

        while (!hasConverged) {
            Job job = Job.getInstance(conf, "KMeans Clustering - Iteration " + (i + 1));

            // Add the seeds (centroids) file to the cache for this job
            job.addCacheFile(new URI(i == 0 ? seedsPath : (outputPathBase + "/iteration_" + i + "/part-r-00000")));

            job.setJarByClass(Task2d.class);

            // Set input path: For the first iteration, use the initial input. For subsequent iterations, use the output of the previous iteration
            FileInputFormat.addInputPath(job, new Path(inputPath));
            // Set output path for this iteration
            Path outputPath = new Path(outputPathBase + "/iteration_" + (i + 1));
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
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
            if (i > 0) {
                hasConverged = checkConvergence(
                        outputPathBase + "/iteration_" + i,
                        outputPathBase + "/iteration_" + (i + 1),
                        0.5
                ); // should this be /iteration_ i + 1
                if (hasConverged) {
                    System.out.println("KMeans Clustering converged after " + (i + 1) + " iterations.");
                    break;
                }
            }
            i++;
        }

        // End time and calculate total time
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Total runtime: " + totalTime + " milliseconds");
    }
}