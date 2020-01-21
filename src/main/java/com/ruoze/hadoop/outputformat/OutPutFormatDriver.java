package com.ruoze.hadoop.outputformat;

import com.ruoze.hadoop.ser.Access;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutPutFormatDriver {

    public static void main(String[] args) {

        try {

        Configuration configuration=new Configuration();
        Job job=job = Job.getInstance(configuration);

        job.setJarByClass(OutPutFormatDriver.class);
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        String intput="inputDir/1.log";
        String output="output";

        job.setOutputFormatClass(MyOutPutFormat.class);
        FileInputFormat.setInputPaths(job,new Path(intput));
        FileOutputFormat.setOutputPath(job,new Path(output));

        job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class WcMapper extends Mapper<LongWritable, Text,Text,NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.write(value,NullWritable.get());
        }
    }

    public static class WcReduce extends Reducer<Text,NullWritable, Text,NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            for (NullWritable value:values) {
                context.write(key,NullWritable.get());
            }

        }
    }
}
