package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;


public class OrderInversion extends Configured implements Tool {

    private final static String ASTERISK = "\0";

    public static class PartitionerTextPair extends Partitioner<TextPair, IntWritable> {

        @Override
        public int getPartition(TextPair key, IntWritable value, int numPartitions) {
            return toUnsigned(key.getFirst().hashCode()) % numPartitions;
        }

        /**
         * toUnsigned(10) = 10
         * toUnsigned(-1) = 2147483647
         *
         * @param val Value to convert
         * @return the unsigned number with the same bits of val
         */
        public static int toUnsigned(int val) {
            return val & Integer.MAX_VALUE;
        }
    }

    public static class PairMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
        private final Text REF = new Text();
        private final Text NEIGHBOR = new Text();
        private final TextPair PAIR = new TextPair();
        private final IntWritable ONE = new IntWritable(1);

        private String[] iteratorToTokenArray(StringTokenizer tok) {
            String[] tokens = new String[tok.countTokens()];
            int i = 0;
            while (tok.hasMoreTokens()) {
                tokens[i++] = tok.nextToken();
            }
            return tokens;
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            StringTokenizer tok = new StringTokenizer(value.toString());
            String[] tokens = iteratorToTokenArray(tok);

            for (String ref : tokens) {
                this.REF.set(ref);
                for (String neighbour : tokens) {
                    if (ref.equals(neighbour)) continue;

                    this.NEIGHBOR.set(neighbour);
                    this.PAIR.set(REF, NEIGHBOR);
                    context.write(this.PAIR, ONE);

                    this.NEIGHBOR.set(ASTERISK);
                    this.PAIR.set(REF, NEIGHBOR);
                    context.write(this.PAIR, ONE);
                }
            }
        }
    }

    public static class PairCombiner extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
        private final IntWritable COUNT = new IntWritable();

        @Override
        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }

            this.COUNT.set(sum);
            context.write(key, this.COUNT);
        }
    }

    public static class PairReducer extends Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {
        private static Map<String, Integer> marginalMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            marginalMap = new HashMap<>();
        }

        @Override
        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String word = key.getFirst().toString();
            if (key.getSecond().toString().equals(ASTERISK)) {
                int sum = 0;
                for (IntWritable i : values) {
                    sum += i.get();
                }

                marginalMap.put(word, sum);
            } else {
                double sum = 0.0;
                for (IntWritable i : values) {
                    sum += i.get();
                }

                int total = marginalMap.get(word);
                context.write(key, new DoubleWritable(sum / total));
            }
        }
    }

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = null;  // TODO: define new job instead of null using conf e setting a name

        // TODO: set job input format
        // TODO: set map class and the map output key and value classes
        // TODO: set reduce class and the reduce output key and value classes
        // TODO: set job output format
        // TODO: add the input file as job input (from HDFS) to the variable inputFile
        // TODO: set the output path for the job results (to HDFS) to the variable outputPath
        // TODO: set the number of reducers using variable numberReducers
        // TODO: set the jar class

        return job.waitForCompletion(true) ? 0 : 1;
    }

    OrderInversion(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: OrderInversion <num_reducers> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrderInversion(args), args);
        System.exit(res);
    }
}
