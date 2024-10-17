import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.Queue;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramCount {

    public static class NgramMapper extends Mapper<Object, Text, Text, MapWritable> {

        private final static IntWritable one = new IntWritable(1);
        private int N;
        private Map<Text, MapWritable> cleanupMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            N = Integer.parseInt(conf.get("N"));
            cleanupMap = new HashMap<>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().replaceAll("[^a-zA-Z0-9]+", " ");
            ArrayList<String> tokens = new ArrayList<>();
            String[] tokensArray = line.split("\\s+");
    
            for (String token : tokensArray) {
                if (!token.isEmpty()) {
                    tokens.add(token);
                }
            }

            for ( int i=0; i<= tokens.size()-N; i++) {
                StringBuilder ngramBuilder = new StringBuilder();
                for (int j = 1; j < N; j++) {
                    if (j > 1) {
                        ngramBuilder.append(" ");
                    }
                    ngramBuilder.append(tokens.get(i + j));
                }

                String prefix = tokens.get(i);
                String followingWords = ngramBuilder.toString();

                Text prefixText = new Text(prefix);
                MapWritable stripe = cleanupMap.getOrDefault(prefixText, new MapWritable());
                Text followingWordsText = new Text(followingWords);

                IntWritable count = (IntWritable) stripe.getOrDefault(followingWordsText, new IntWritable(0));
                count.set(count.get() + 1);
                stripe.put(followingWordsText, count);

                cleanupMap.put(prefixText, stripe);
            }
        }

            @Override
            protected void cleanup(Context context) throws IOException, InterruptedException {
                for (Map.Entry<Text, MapWritable> entry : cleanupMap.entrySet()) {
                    context.write(entry.getKey(), entry.getValue());
                }
            }
    }

    public static class IntSumReducer extends Reducer<Text, MapWritable, Text, IntWritable> {
        private MapWritable countMap = new MapWritable();
        private Text cleanupKey = new Text();

        //key: n-gram, value: count, 
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException { 
            cleanupKey = key;

            for (MapWritable stripe : values) {        //sum up the values of the same key
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    Text  currentWord = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();
                    IntWritable storedCount = (IntWritable) countMap.getOrDefault(currentWord, new IntWritable(0));
                    storedCount.set(storedCount.get() + count.get());
                    countMap.put(currentWord, storedCount);
                }
            }

            for (Map.Entry<Writable, Writable> entry : countMap.entrySet()) {
                        Text nextWord = (Text) entry.getKey();
                        context.write(new Text(cleanupKey.toString() + " " + nextWord.toString()), (IntWritable) entry.getValue());
                    }
        }
        // @Override
        // protected void cleanup(Context context) throws IOException, InterruptedException {
        //     for (Map.Entry<Writable, Writable> entry : countMap.entrySet()) {
        //         Text nextWord = (Text) entry.getKey();
        //         // int outputCount = ((IntWritable) entry.getValue()).get();
        //         context.write(new Text(cleanupKey.toString() + " " + nextWord.toString()), (IntWritable) entry.getValue());
        //     }
        // }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int N = Integer.parseInt(args[2]);
        conf.set("N", args[2]);
        conf.setInt("ngram.size", N);

        Job job = Job.getInstance(conf, "N-gram count");
        job.setJarByClass(NgramCount.class); // later rename
        job.setMapperClass(NgramMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}