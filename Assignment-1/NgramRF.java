import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.ArrayList;

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

    public static class InMapperCombiningNgramMapper extends Mapper<Object, Text, Text, MapWritable> {
        private int ngramSize;
        private Map<Text, MapWritable> intermediateMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            ngramSize = Integer.parseInt(conf.get("N"));
            intermediateMap = new HashMap<>();
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

                // Update the stripe for in-mapper combining
                Text prefixText = new Text(prefix);
                MapWritable stripe = intermediateMap.getOrDefault(prefixText, new MapWritable());
                Text followingWordsText = new Text(followingWords);
                IntWritable count = (IntWritable) stripe.getOrDefault(followingWordsText, new IntWritable(0));
                count.set(count.get() + 1);
                stripe.put(followingWordsText, count);

                // Update the count for "*"
                IntWritable totalCount = (IntWritable) stripe.getOrDefault(new Text("*"), new IntWritable(0));
                totalCount.set(totalCount.get() + 1);
                stripe.put(new Text("*"), totalCount);

                // Update the intermediate map
                intermediateMap.put(prefixText, stripe);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit all combined intermediate results
            for (Map.Entry<Text, MapWritable> entry : intermediateMap.entrySet()) {
                context.write(entry.getKey(), entry.getValue());
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
        job.setJarByClass(NgramRF.class);

        // Set Mapper, Combiner, and Reducer classes
        job.setMapperClass(InMapperCombiningNgramMapper.class);
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
