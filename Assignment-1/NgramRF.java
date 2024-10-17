import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
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

    public static class NgramMapper extends Mapper<Object, Text, Text, MapWritable> {
        private int N;
        private final Text prefixText = new Text();
        private final MapWritable stripe = new MapWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            N = Integer.parseInt(conf.get("N"));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().replaceAll("[^a-zA-Z0-9]+", " ");
            String[] tokensArray = line.split("\\s+");

            for (int i = 0; i <= tokensArray.length - N; i++) {
                StringBuilder ngramBuilder = new StringBuilder();
                for (int j = 1; j < N; j++) {
                    if (j > 1) {
                        ngramBuilder.append(" ");
                    }
                    ngramBuilder.append(tokensArray[i + j]);
                }

                String prefix = tokensArray[i];
                String followingWords = ngramBuilder.toString();

                prefixText.set(prefix);
                stripe.clear();
                stripe.put(new Text(followingWords), new IntWritable(1));
                stripe.put(new Text("*"), new IntWritable(1));

                context.write(prefixText, stripe);
            }
        }
    }

    public static class NgramCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable combinedStripe = new MapWritable();

            for (MapWritable stripe : values) {
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    Text followingWords = (Text) entry.getKey();
                    IntWritable value = (IntWritable) entry.getValue();
                    IntWritable existingValue = (IntWritable) combinedStripe.getOrDefault(followingWords, new IntWritable(0));
                    existingValue.set(existingValue.get() + value.get());
                    combinedStripe.put(followingWords, existingValue);
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

            for (MapWritable stripe : values) {
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    Text followingWords = (Text) entry.getKey();
                    IntWritable value = (IntWritable) entry.getValue();
                    IntWritable existingValue = (IntWritable) aggregateStripe.getOrDefault(followingWords, new IntWritable(0));
                    existingValue.set(existingValue.get() + value.get());
                    aggregateStripe.put(followingWords, existingValue);
                }
            }

            totalCount = ((IntWritable) aggregateStripe.get(new Text("*"))).get();

            for (Map.Entry<Writable, Writable> entry : aggregateStripe.entrySet()) {
                Text nextWord = (Text) entry.getKey();
                if (!nextWord.toString().equals("*")) {
                    int count = ((IntWritable) entry.getValue()).get();
                    float frequency = (float) count / totalCount;

                    if (frequency >= theta) {
                        relativeFrequency.set(frequency);
                        context.write(new Text(key.toString() + " " + nextWord.toString()), relativeFrequency);
                    }
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
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "N-gram Relative Frequency");
        job.setJarByClass(NgramRF.class);

        job.setMapperClass(NgramMapper.class);
        job.setCombinerClass(NgramCombiner.class);
        job.setReducerClass(RelativeFrequencyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



