import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramRF {

    public static class NgramMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private int N;
        private Queue<String> words = new LinkedList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            N = conf.getInt("ngram.size", 2);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] wordsOnThisLine = line.split("[^a-zA-Z0-9]+");

            for (String word : wordsOnThisLine) {
                words.offer(word);
                if (words.size() == N) {
                    String ngramKey = String.join(" ", words);  // concatenate words in the queue to form an n-gram
                    context.write(new Text(ngramKey), one); // record the n-gram
                    String wordKey = words.poll() + " *";  // record the n-gram start by the first word
                    context.write(new Text(wordKey), one);  //record the n-gram start by the first word
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, Text> {
        private HashMap<String, Integer> totalCounts = new HashMap<>();
        private double theta;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            theta = conf.getFloat("theta", 0.0f);
        }

        //key: n-gram, value: count, 
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
            String keyStr = key.toString();
            int sum = 0;

            for (IntWritable val : values) {        //sum up the values of the same key
                sum += val.get();   
            }

            if (keyStr.endsWith(" *")) {
                // the total no. of word count
                String word = keyStr.split(" ")[0];     // get the first word
                totalCounts.put(word, sum);     // record the total no. of word count
            } else {
                // the total no. of n-gram count
                String firstWord = keyStr.split(" ")[0];
                int totalCount = totalCounts.getOrDefault(firstWord, 1);
                double relativeFrequency = (double) sum / totalCount;

                if (relativeFrequency >= theta) {
                    context.write(key, new Text(String.valueOf(relativeFrequency)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: NgramRF <input path> <output path> <N> <theta>");
            System.exit(-1);
        }

        int N = Integer.parseInt(args[2]);
        float theta = Float.parseFloat(args[3]);

        if (theta < 0 || theta > 1) {
            System.err.println("Theta must be between 0 and 1");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("ngram.size", N);
        conf.setFloat("theta", theta);

        Job job = Job.getInstance(conf, "N-gram relative frequency");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(NgramMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}