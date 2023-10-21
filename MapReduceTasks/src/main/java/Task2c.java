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

public class Task2c {

    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text centroidKey = new Text();
        private Text pointValue = new Text();
        private ArrayList<ConvergenceChecker.Point<Integer, Integer>> seeds = new ArrayList<>();

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
                ConvergenceChecker.Point<Integer, Integer> seed = new ConvergenceChecker.Point<>(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]));
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
            ConvergenceChecker.Point<Integer, Integer> currentCentroid = null;

            for (ConvergenceChecker.Point<Integer, Integer> seed : seeds) {
                double distance = Math.sqrt(Math.pow(x - seed.x, 2) + Math.pow(y - seed.y, 2));
                if (distance < currentMinDist) {
                    currentMinDist = distance;
                    currentCentroid = seed;
                }
            }
            return currentCentroid.x + "," + currentCentroid.y;
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, NullWritable> {

        /**
         * The reduce function receives a key-value pair where the key is a centroid and the values are the list of points that are closest to that centroid.
         */
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

            context.write(newCentroid, NullWritable.get()); // return just new centroid as we are setting the value to null
        }
    }

    public static void main(String[] args) throws Exception {

        // Start time
        long startTime = System.currentTimeMillis();


        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String inputPath = "/user/ds503/input_project_2/data_points.csv";
        String seedsPath = "/user/ds503/input_project_2/20seed_points.csv";
        String outputPathBase = "/user/ds503/output_project_2/task_2c/k_20_r_20";

        boolean hasConverged = false;

        int maxIterations = 20;

        for (int i=0; i < maxIterations; i++) {
            Job job = Job.getInstance(conf, "KMeans Clustering - Iteration " + (i + 1));

            // Add the seeds (centroids) file to the cache for this job
            job.addCacheFile(new URI(i == 0 ? seedsPath : (outputPathBase + "/iteration_" + i + "/part-r-00000")));

            job.setJarByClass(Task2c.class);

            // Set input path: For the first iteration, use the initial input. For subsequent iterations, use the output of the previous iteration
            FileInputFormat.addInputPath(job, new Path(inputPath));
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

            // Running the map reduce job for the iteration
            boolean success = job.waitForCompletion(true);
            if (!success) {
                System.out.println("KMeans Clustering failed on iteration " + (i + 1));
                System.exit(1);
            }

            //Check for convergence by comparing the centroids from this iteration to the previous iteration. Break if it has converged
            if (i > 0) {
                hasConverged = ConvergenceChecker.checkConvergence(
                        outputPathBase + "/iteration_" + i + "/part-r-00000",
                        outputPathBase + "/iteration_" + (i + 1) + "/part-r-00000",
                        0.5
                ); // should this be /iteration_ i + 1
                if (hasConverged) {
                    System.out.println("KMeans Clustering converged after " + (i + 1) + " iterations.");
                    break;
                }
            }
        }

        // End time and calculate total time
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Total runtime: " + totalTime + " milliseconds");

        if(hasConverged) {
            System.out.println("The KMeans clustering has converged.");
        } else {
            System.out.println("The KMeans clustering has NOT converged after " + maxIterations + " iterations.");
        }
    }
}