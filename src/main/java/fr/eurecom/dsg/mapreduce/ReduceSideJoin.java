package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class ReduceSideJoin extends Configured implements Tool {

    private Path outputDir;
    private Path inputPath;
    private int numReducers;

    private static final int INCOMING = -1;
    private static final int OUTGOING = 1;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "EmilFabian ReduceSideJoin");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntPair.class);

        job.setReducerClass(ReduceSideJoinReducer.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setNumReduceTasks(numReducers);
        job.setJarByClass(ReduceSideJoin.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ReduceSideJoinMapper extends Mapper<LongWritable, Text, IntWritable, IntPair> {
        private IntPair connection = new IntPair();
        private IntWritable outputKey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] userIDs = value.toString().split("\\s+");

            final int user = Integer.valueOf(userIDs[0]);
            final int follower = Integer.valueOf(userIDs[1]);

            outputKey.set(user);
            connection.set(follower, OUTGOING);
            context.write(outputKey, connection);

            outputKey.set(follower);
            connection.set(user, INCOMING);
            context.write(outputKey, connection);
        }
    }

    public static class ReduceSideJoinReducer extends Reducer<IntWritable, IntPair, IntPair, IntWritable> {
        private Set<Integer> incoming = new HashSet<>();
        private Set<Integer> outgoing = new HashSet<>();

        @Override
        protected void reduce(IntWritable key, Iterable<IntPair> values, Context context) throws IOException, InterruptedException {
            for (IntPair pair : values) {
                if (pair.second == INCOMING) {
                    incoming.add(pair.first);
                } else {
                    outgoing.add(pair.first);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            IntPair key = new IntPair();
            IntWritable value = new IntWritable();

            for (int in : incoming) {
                for (int out : outgoing) {
                    key.set(in, out);
                    context.write(key, value);
                }
            }

            incoming.clear();
            outgoing.clear();

            super.cleanup(context);
        }
    }

    public static class IntPair implements Writable {
        private int first;
        private int second;

        public IntPair() { }

        public IntPair(int first, int second) {
            this.first = first;
            this.second = second;
        }

        public void setFirst(int first) {
            this.first = first;
        }

        public void setSecond(int second) {
            this.second = second;
        }

        public void set(int first, int second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(this.first);
            dataOutput.writeInt(this.second);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.first = dataInput.readInt();
            this.second = dataInput.readInt();
        }
    }

    public ReduceSideJoin(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: ReduceSideJoin <num_reducers> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReduceSideJoin(args), args);
        System.exit(res);
    }

}