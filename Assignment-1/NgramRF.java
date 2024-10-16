import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramRF {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
        private int ngramSize;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            ngramSize = Integer.parseInt(conf.get("N"));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            ArrayList<String> tokens = new ArrayList<>();

            // Extract tokens, removing non-alphabetic characters
            while (itr.hasMoreTokens()) {
                tokens.add(itr.nextToken().replaceAll("[^a-zA-Z]", ""));
            }

            // Iterate over the tokens to create n-grams
            for (int i = 0; i <= tokens.size() - ngramSize; i++) {
                StringBuilder ngramBuilder = new StringBuilder();
                for (int j = 1; j < ngramSize; j++) {
                    if (j > 1) {
                        ngramBuilder.append(" ");
                    }
                    ngramBuilder.append(tokens.get(i + j));
                }

                String prefix = tokens.get(i);
                String followingWords = ngramBuilder.toString();

                // Create stripe with followingWords and count
                MapWritable stripe = new MapWritable();
                Text followingWordsText = new Text(followingWords);
                IntWritable count = new IntWritable(1);
                stripe.put(followingWordsText, count);

                // Add "*" to track the total number of n-grams that start with the prefix
                stripe.put(new Text("*"), count);

                // Emit prefix and stripe
                context.write(new Text(prefix), stripe);
            }
        }
    }

    public static class Combiner extends Reducer<Text, MapWritable, Text, MapWritable> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable combinedStripe = new MapWritable();
            for (MapWritable stripe : values) {
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    Text nextWord = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();
                    IntWritable combinedCount = (IntWritable) combinedStripe.getOrDefault(nextWord, new IntWritable(0));
                    combinedCount.set(combinedCount.get() + count.get());
                    combinedStripe.put(nextWord, combinedCount);
                }
            }
            context.write(key, combinedStripe);
        }
    }

    public static class RelativeFrequencyReducer extends Reducer<Text, MapWritable, Text, FloatWritable> {
        private FloatWritable relativeFrequency = new FloatWritable();
        private float theta;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            theta = Float.parseFloat(conf.get("theta"));
        }

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable aggregateStripe = new MapWritable();
            int totalCount = 0;

            // Aggregate all stripes
            for (MapWritable stripe : values) {
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    Text nextWord = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();
                    if (nextWord.toString().equals("*")) {
                        totalCount += count.get();
                    } else {
                        IntWritable currentCount = (IntWritable) aggregateStripe.getOrDefault(nextWord, new IntWritable(0));
                        currentCount.set(currentCount.get() + count.get());
                        aggregateStripe.put(nextWord, currentCount);
                    }
                }
            }

            // Calculate relative frequencies and emit results if frequency >= theta
            for (Map.Entry<Writable, Writable> entry : aggregateStripe.entrySet()) {
                Text nextWord = (Text) entry.getKey();
                int count = ((IntWritable) entry.getValue()).get();
                float frequency = (float) count / totalCount;

                if (frequency >= theta) {
                    relativeFrequency.set(frequency);
                    context.write(new Text(key.toString() + " " + nextWord.toString()), relativeFrequency);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: hadoop jar [jarfile] [class name] [input dir] [output dir] [N] [theta]");
            System.exit(-1);
        }

        int N = Integer.parseInt(args[2]);
        float theta = Float.parseFloat(args[3]);

        if (theta < 0 || theta > 1) {
            System.err.println("Theta must be between 0 and 1");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("N", args[2]);
        conf.set("theta", args[3]);
    
        Job job = Job.getInstance(conf, "N-gram Relative Frequency");
        job.setJarByClass(NgramRF.class); // Ensure Hadoop uses the correct jar with your class
    
        // Set Mapper, Combiner, and Reducer classes
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(RelativeFrequencyReducer.class);

        // Set key/value classes for Mapper and Reducer outputs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}