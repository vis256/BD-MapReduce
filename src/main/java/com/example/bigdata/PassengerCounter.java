package com.example.bigdata;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

public class PassengerCounter extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PassengerCounter(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "PassengerCounter");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PassengerMapper.class);
        job.setReducerClass(PassengerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class PassengerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() != 0) {
                    String line = lineText.toString();
                    PassengerData pd = PassengerParser.parseLine(line);
                    if (pd != null) {
                        Pair<String, Integer> data = pd.toKeyValue();
                        context.write(new Text( data.getKey() ), new IntWritable(data.getValue() ));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class PassengerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        int counter;
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            counter = 0;

            for (IntWritable val : values) {
                counter += val.get();
            }

            context.write(key, new IntWritable( counter ));
        }
    }
}