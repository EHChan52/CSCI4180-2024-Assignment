import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramRF {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
        private int N;
        private Map<String, MapWritable> ngramCounts = new HashMap<>();
        private Text prefixKey = new Text();
    
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            N = Integer.parseInt(conf.get("N"));
        }
    
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            StringTokenizer itr = new StringTokenizer(line);
            ArrayList<String> tokens = new ArrayList<>();
            
            while (itr.hasMoreTokens()) {
                tokens.add(itr.nextToken().replaceAll("[^a-zA-Z]", ""));
            }
    
            for (int i = 0; i <= tokens.size() - N; i++) {
                StringBuilder ngramBuilder = new StringBuilder();
                for (int j = 0; j < N - 1; j++) {
                    if (j > 0) ngramBuilder.append(" ");
                    ngramBuilder.append(tokens.get(i + j));
                }
    
                String prefix = ngramBuilder.toString();
                String nextWord = tokens.get(i + N - 1);
    
                MapWritable stripe = ngramCounts.getOrDefault(prefix, new MapWritable());
                Text nextWordText = new Text(nextWord);
                IntWritable count = (IntWritable) stripe.getOrDefault(nextWordText, new IntWritable(0));
                count.set(count.get() + 1);
                stripe.put(nextWordText, count);
                ngramCounts.put(prefix, stripe);
            }
        }
    
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, MapWritable> entry : ngramCounts.entrySet()) {
                prefixKey.set(entry.getKey());
                context.write(prefixKey, entry.getValue());
            }
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
    
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable aggregateStripe = new MapWritable();
            int totalCount = 0;
    
            // Aggregate all MapWritable stripes
            for (MapWritable stripe : values) {
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    Text nextWord = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();
                    
                    IntWritable currentCount = (IntWritable) aggregateStripe.getOrDefault(nextWord, new IntWritable(0));
                    currentCount.set(currentCount.get() + count.get());
                    aggregateStripe.put(nextWord, currentCount);
                    totalCount += count.get();
                }
            }
    
            // Calculate relative frequencies and write if frequency >= theta
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
        if (args.length < 5) {
            System.err.println("Usage: hadoop jar [jarfile] [class name] [input dir] [output dir] [N] [theta]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("N", args[3]);
        conf.set("theta", args[4]);
        Job job = Job.getInstance(conf, "N-gram Relative Frequency");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(RelativeFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
