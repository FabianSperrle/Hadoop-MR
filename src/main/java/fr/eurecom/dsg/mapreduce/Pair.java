package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Pair extends Configured implements Tool {

    public static class PairMapper
            extends Mapper<LongWritable,
            Text,
            TextPair,
            IntWritable> {

        private IntWritable ONE;

        private String[] iteratorToTokenArray(StringTokenizer tok) {
            String[] tokens = new String[tok.countTokens()];
            int i = 0;
            while (tok.hasMoreTokens()) {
                tokens[i++] = tok.nextToken();
            }
            return tokens;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.ONE = new IntWritable(1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tok = new StringTokenizer(value.toString());
            String[] tokens = iteratorToTokenArray(tok);

            for (int i = 0; i < tokens.length; i++) {
                String ref = tokens[i];
                for (int j = 0; j < tokens.length; j++) {
                    String neighbour = tokens[j];
                    if (ref.equals(neighbour)) {
                        continue;
                    }
                    TextPair tp = new TextPair(ref, neighbour);
                    context.write(tp, ONE);
                }
            }

        }
    }

    public static class PairReducer
            extends Reducer<TextPair,
            IntWritable,
            TextPair,
            IntWritable> {

        private IntWritable INT;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.INT = new IntWritable();
        }

        @Override
        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }

            INT.set(sum);
            context.write(key, INT);
        }
    }

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    public Pair(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Pair <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }


    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Job job = new Job(conf, "EmilFabian PAIR");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(PairMapper.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(PairReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, this.inputPath);

        FileOutputFormat.setOutputPath(job, this.outputDir);

        job.setNumReduceTasks(this.numReducers);

        job.setJarByClass(Pair.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Pair(args), args);
        System.exit(res);
    }
}
