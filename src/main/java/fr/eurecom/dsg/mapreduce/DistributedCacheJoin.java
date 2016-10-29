package fr.eurecom.dsg.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedCacheJoin extends Configured implements Tool {

    private Path outputDir;
    private Path inputFile;
    private Path inputTinyFile;
    private int numReducers;

    public DistributedCacheJoin(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: DistributedCacheJoin <num_reducers> " +
                    "<input_tiny_file> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputTinyFile = new Path(args[1]);
        this.inputFile = new Path(args[2]);
        this.outputDir = new Path(args[3]);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        DistributedCache.addCacheFile(this.inputTinyFile.toUri(), conf);

        Job job = new Job(conf, "FabianEmil distr. cache join");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(DistributedCacheMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(DistributedCacheReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, this.inputFile);
        FileOutputFormat.setOutputPath(job, this.outputDir);

        job.setNumReduceTasks(this.numReducers);
        job.setJarByClass(DistributedCacheJoin.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new DistributedCacheJoin(args),
                args);
        System.exit(res);
    }

}

class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private HashSet<String> stopwords;
    private IntWritable ONE;
    private Text TMP_TEXT;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.stopwords = new HashSet<>();
        this.ONE = new IntWritable(1);
        this.TMP_TEXT = new Text();

        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        FileSystem fs = FileSystem.getLocal(context.getConfiguration());
        Path stopWordPath = new Path(cacheFiles[0].toString());

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(stopWordPath)));

        String line;
        while ((line = reader.readLine()) != null) {
            stopwords.add(line);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);

        StringTokenizer tok = new StringTokenizer(value.toString());
        while (tok.hasMoreTokens()) {
            String token = tok.nextToken();
            if (!this.stopwords.contains(token)) {
                TMP_TEXT.set(token);
                context.write(TMP_TEXT, ONE);
            }
        }
    }
}

class DistributedCacheReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable COUNT = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        this.COUNT.set(sum);
        context.write(key, COUNT);
    }
}

