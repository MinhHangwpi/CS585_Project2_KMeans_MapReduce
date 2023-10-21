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

public class Task2a {

    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text centroidKey = new Text();
        private Text pointValue = new Text();
        private ArrayList<ConvergenceChecker.Point<Integer, Integer>> seeds = new ArrayList<>();

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
                ConvergenceChecker.Point<Integer, Integer> seed = new ConvergenceChecker.Point<>(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]));
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

            context.write(newCentroid, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // clean up output directories if they exist currently
        FileSystem fs = FileSystem.get(conf);

        String inputPath = "/user/ds503/input_project_2/data_points.csv";
        String seedsPath = "/user/ds503/input_project_2/10seed_points.csv";
        String outputPath = "/user/ds503/output_project_2/task_2a/k_10";

        Path outPath = new Path(outputPath); // output path
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        Job job = Job.getInstance(conf, "KMeans Clustering");
        Path inputAllDataPath = new Path(inputPath);

        job.addCacheFile(new URI(seedsPath)); // path to the initial seed point files
        job.setJarByClass(Task2a.class);

        FileInputFormat.addInputPath(job, inputAllDataPath); // path to the full data set
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        System.out.println("Job execution time for Task 2a - Single K-Means iteration: " + elapsedTime + " milliseconds.");

        // check for convergence by comparing the initial seed points with the updated centroids
        boolean hasConverged = ConvergenceChecker.checkConvergence(
                seedsPath,
                outputPath + "/part-r-00000",
                0.5
        );

        if (hasConverged){
            System.out.println("Have reached convergence!");
        } else {
            System.out.println("Have not reached convergence yet!");
        }
        System.exit(success ? 0 : 1);
    }
}