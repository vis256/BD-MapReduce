package com.example.bigdata;
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

        private final Text year = new Text();
        private final IntWritable size = new IntWritable();

        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() != 0) {
                    String line = lineText.toString();
                    int i = 0;
                    /*
                    2,2018-12-12 15:13:24,2018-12-12 15:50:10,1,5.99,1,N,231,237,1,27,0,0.5,5.56,0,0.3,33.36
                    [1] - Entering Cab Date
                    [3] - Passenger Count
                    [7] - Entering Zone
                    [9] - Payment Method
                     */
                    for (String word : line
                            .split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
                        i++;
                    }
                    //TODO: write intermediate pair to the context

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class PassengerReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private final DoubleWritable resultValue = new DoubleWritable();
        Float average;
        Float count;
        int sum;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            average = 0f;
            count = 0f;
            sum = 0;

            Text resultKey = new Text("average station size in " + key + " was: ");

            for (IntWritable val : values) {
                sum += val.get();
                count += 1;
            }
            //TODO: set average variable properly

            resultValue.set(average);
            //TODO: write result pair to the context

        }
    }
}