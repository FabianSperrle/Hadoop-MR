package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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


public class Stripes extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Job job = new Job(conf, "EmilFabian Stripes");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(StripesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringToIntMapWritable.class);

        job.setReducerClass(StripesReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, this.inputPath);

        FileOutputFormat.setOutputPath(job, this.outputDir);

        job.setNumReduceTasks(this.numReducers);

        job.setJarByClass(Stripes.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public Stripes(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Stripes <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
        System.exit(res);
    }
}

class StripesMapper extends Mapper<LongWritable, Text, Text, StringToIntMapWritable> {

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
        Map<String, Map<String, Integer>> map = new HashMap<>();

        StringTokenizer tok = new StringTokenizer(value.toString());
        String[] tokens = iteratorToTokenArray(tok);

        for (String ref : tokens) {
            Map<String, Integer> innerMap;
            if (map.containsKey(ref)) {
                innerMap = map.get(ref);
            } else {
                innerMap = new HashMap<>();
            }
            for (String neighbour : tokens) {
                if (ref.equals(neighbour)) {
                    continue;
                }

                if (innerMap.containsKey(neighbour)) {
                    innerMap.put(neighbour, innerMap.get(neighbour) + 1);
                } else {
                    innerMap.put(neighbour, 1);
                }
            }
            map.put(ref, innerMap);
        }

        Text TMP_TEXT = new Text();
        StringToIntMapWritable TMP_SIM = new StringToIntMapWritable();
        for (Map.Entry<String, Map<String, Integer>> entry : map.entrySet()) {
            TMP_TEXT.set(entry.getKey());
            TMP_SIM.set(entry.getValue());

            context.write(TMP_TEXT, TMP_SIM);
        }
    }
}

class StripesReducer extends Reducer<Text, StringToIntMapWritable, TextPair, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<StringToIntMapWritable> values, Context context) throws IOException, InterruptedException {
        HashMap<TextPair, Integer> map = new HashMap<>();

        for (StringToIntMapWritable sim_map : values) {
            for (Map.Entry<String, Integer> entry : sim_map.getMap().entrySet()) {
                Text text = new Text(entry.getKey());
                TextPair tp = new TextPair(key, text);
                int count = entry.getValue();

                if (map.containsKey(tp)) {
                    map.put(tp, map.get(tp) + count);
                } else {
                    map.put(tp, count);
                }
            }
        }

        IntWritable TMP_INT = new IntWritable();
        for (Map.Entry<TextPair, Integer> entry : map.entrySet()) {
            TMP_INT.set(entry.getValue());
            context.write(entry.getKey(), TMP_INT);
        }
    }
}