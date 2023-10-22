import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class SilhouetteCoefficient {

    public static class SilhouetteMapperOne extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text output = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length > 1) {
                String centroid = parts[0];
                String point = parts[1];
                outKey.set(centroid);
                output.set(point);
                context.write(outKey, output);
            }
        }
    }

    public static class SilhouetteReducerOne extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder valueList = new StringBuilder();
            for (Text value : values) {
                if (valueList.length() > 0) {
                    valueList.append("\t");
                }
                valueList.append(new Text("(" + value + ")"));
            }
            context.write(new Text("1"), new Text("" + valueList));
        }
    }

    public static int COUNTER_NAME = 0;

    public static class ValueMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text output = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 2) {
                Text mainKey = new Text(parts[0]);
                StringBuilder values = new StringBuilder();
                for (int i = 1; i < parts.length; i++) {
                    if (values.length() > 0) {
                        values.append("\t");
                    }
                    values.append(parts[i].replace("(", "").replace(")", ""));
                }
                outKey.set(mainKey);
                output.set(values.toString());
                context.write(outKey, output);
                COUNTER_NAME++;
            }
        }
    }

    public static class ValueReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> allValues = new ArrayList<>();
            for (Text value : values) {
                allValues.add(value.toString());
            }
            context.write(new Text("1"), new Text(allValues.toString()));
        }
    }

    public static class SilhouetteMapperTwo extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text output = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                String[] centroidCoords = parts[0].split(",");
                String[] pointCoords = parts[1].split(",");
                outKey.set(centroidCoords[0] + "," + centroidCoords[1]);
                output.set("P:" + pointCoords[0] + "," + pointCoords[1]);
                context.write(outKey, output);
            }
        }
    }

    public static class SilhouetteMapperThree extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String modifiedValue = value.toString().substring(3, value.toString().length() - 1);
            context.write(new Text("0A0A,0A0A"), new Text("A:" + modifiedValue));
        }
    }

    public static class SilhouetteReducerTwo extends Reducer<Text, Text, Text, DoubleWritable> {
        private double calculateDistance(String point1, String point2) {
            String[] coord1 = point1.split(",");
            String[] coord2 = point2.split(",");
            double x1 = Double.parseDouble(coord1[0]);
            double y1 = Double.parseDouble(coord1[1]);
            double x2 = Double.parseDouble(coord2[0]);
            double y2 = Double.parseDouble(coord2[1]);
            return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
        }

        private double calculateA(String pointCoord, List<String> pointCoords) {
            double sum = 0.0;
            int count = 0;
            for (String otherPointCoord : pointCoords) {
                if (!otherPointCoord.equals(pointCoord)) {
                    double distance = calculateDistance(pointCoord, otherPointCoord);
                    sum += distance;
                    count++;
                }
            }
            return (count > 0) ? sum / count : 0.0;
        }

        private double calculateSilhouette(String pointCoord, List<String> pointCoords) {
            double a_i = calculateA(pointCoord, pointCoords);
            double b_i = calculateB(pointCoord);
            return (b_i - a_i) / Math.max(a_i, b_i);
        }

        private static final List<String> allPoints = new ArrayList<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> pointCoords = new ArrayList<>();
            for (Text value : values) {
                String valueStr = value.toString();
                if (valueStr.startsWith("P:")) {
                    pointCoords.add(valueStr.substring(2));
                } else if (valueStr.startsWith("A:")) {
                    allPoints.add(valueStr.substring(2)); // Add to the class-level list, not a local StringBuilder
                }
            }
            for (String pointCoord : pointCoords) {
                double silhouette = calculateSilhouette(pointCoord, pointCoords);
                context.write(new Text(pointCoord), new DoubleWritable(silhouette));
            }
        }


        private double calculateB(String pointCoord) {
            double sum = 0.0;
            int count = 0;
            double minAvgDistance = Double.MAX_VALUE;
            for (String otherPointCoord : SilhouetteReducerTwo.allPoints) {
                String[] pointPairs = otherPointCoord.split(",");
                for (String pointPair : pointPairs) {
                    String[] pointPairCoords = pointPair.split("\t");
                    for (int i = 0; i < pointPairCoords.length; i += 2) {
                        double distance = calculateDistance(pointCoord, pointPairCoords[i] + "," + pointPairCoords[i + 1]);
                        if (distance == 0) {
                            distance = Double.MAX_VALUE;
                        }
                        sum += distance;
                        count++;
                    }
                    double avgDistance = count > 0 ? sum / count : 0.0;
                    sum = 0.0;
                    count = 0;
                    if (avgDistance < minAvgDistance) {
                        minAvgDistance = avgDistance;
                    }
                }
            }
            return minAvgDistance;
        }
    }

    public static class SilhouetteMapperFour extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length > 2) {
                double column3Value = Double.parseDouble(parts[2]);
                context.write(new Text("Silhouette Coefficient for k = " + COUNTER_NAME), new DoubleWritable(column3Value));
            }
        }
    }

    public static class SilhouetteReducerFour extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }
            if (count > 0) {
                double average = sum / count;
                context.write(key, new Text(String.format("%.4f", average)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
//        if (args.length != 6) {
//            System.err.println("Usage: <className> <path_to_input> <path_to_temp_output_1> <path_to_temp_output_2> <path_to_temp_output_3> <path_to_output_final>");
//            System.exit(1);
//        }
        String inputStringPath = "/user/ds503/input_project_2/silhouette/5_seeds";
        String outputTempPath1 = "/user/ds503/output_project_2/silhouette/5_seeds/output_temp_1";
        String outputTempPath2 = "/user/ds503/output_project_2/silhouette/5_seeds/output_temp_2";
        String outputTempPath3 = "/user/ds503/output_project_2/silhouette/5_seeds/output_temp_3";
        String outputFinalPath = "/user/ds503/output_project_2/silhouette/5_seeds/final_output";

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job1 = Job.getInstance(conf, "Grouping by ID");
        FileSystem fs = FileSystem.get(conf);
        Path InputPath = new Path(inputStringPath);
        Path tempOutputPath1 = new Path(outputTempPath1);
        if (fs.exists(tempOutputPath1)) {
            fs.delete(tempOutputPath1, true);
        }
        Path tempOutputPath2 = new Path(outputTempPath2);
        if (fs.exists(tempOutputPath2)) {
            fs.delete(tempOutputPath2, true);
        }
        Path finalOutputPath = new Path(outputTempPath3); // why isn't this named tempOutputPath3 since we have a "super final output" below?
        if (fs.exists(finalOutputPath)) {
            fs.delete(finalOutputPath, true);
        }
        Path superFinalOutputPath = new Path(outputFinalPath);
        if (fs.exists(superFinalOutputPath)) {
            fs.delete(superFinalOutputPath, true);
        }
        job1.setJarByClass(SilhouetteCoefficient.class);
        job1.setMapperClass(SilhouetteMapperOne.class);
        job1.setReducerClass(SilhouetteReducerOne.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, InputPath);
        FileOutputFormat.setOutputPath(job1, tempOutputPath1);
        boolean job1Success = job1.waitForCompletion(true);
        if (job1Success) {
            // Create the second MapReduce job for joining the results
            Job job2 = Job.getInstance(conf, "Get List of All Points");
            job2.setJarByClass(SilhouetteCoefficient.class);
            job2.setMapperClass(ValueMapper.class);
            job2.setReducerClass(ValueReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, tempOutputPath1);
            FileOutputFormat.setOutputPath(job2, tempOutputPath2);
            boolean job2success = job2.waitForCompletion(true);
            if (job2success) {
                Job job3 = Job.getInstance(conf, "Silhouette Coefficient for each point");
                job3.setJarByClass(SilhouetteCoefficient.class);
                job3.setReducerClass(SilhouetteReducerTwo.class);
                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(Text.class);
                MultipleInputs.addInputPath(job3, tempOutputPath2, TextInputFormat.class, SilhouetteCoefficient.SilhouetteMapperThree.class);
                MultipleInputs.addInputPath(job3, InputPath, TextInputFormat.class, SilhouetteCoefficient.SilhouetteMapperTwo.class);
                FileOutputFormat.setOutputPath(job3, finalOutputPath);
                boolean job3success = job3.waitForCompletion(true);
                if (job3success) {
                    Job job4 = Job.getInstance(conf, "Average Silhouette Coefficient");
                    job4.setJarByClass(SilhouetteCoefficient.class);
                    job4.setMapperClass(SilhouetteMapperFour.class);
                    job4.setReducerClass(SilhouetteReducerFour.class);
                    job4.setOutputKeyClass(Text.class);
                    job4.setOutputValueClass(DoubleWritable.class);
                    FileInputFormat.addInputPath(job4, finalOutputPath);
                    FileOutputFormat.setOutputPath(job4, superFinalOutputPath);
                    boolean job4success = job4.waitForCompletion(true);

                    // Calculate and print the elapsed time after job2 finishes
                    long endTime = System.currentTimeMillis();
                    long elapsedTime = endTime - startTime;

                    long milliseconds = elapsedTime % 1000;
                    long seconds = (elapsedTime / 1000) % 60;
                    long minutes = (elapsedTime / (1000 * 60)) % 60;

                    StringBuilder output = new StringBuilder("Total execution time is ");
                    if (minutes > 0) {
                        output.append(minutes).append(" minutes, ");
                    }
                    if (seconds > 0) {
                        output.append(seconds).append(" seconds and ");
                    }
                    output.append(milliseconds).append(" milliseconds.");

                    System.out.println(output);
                    System.exit(job4success ? 0 : 1);
                } else {
                    System.exit(1);
                }
            }
        }
    }
}
