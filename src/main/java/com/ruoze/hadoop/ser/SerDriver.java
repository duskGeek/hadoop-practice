package com.ruoze.hadoop.ser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SerDriver {

    public static void main(String[] args) {

        try {

        Configuration configuration=new Configuration();
        Job job=job = Job.getInstance(configuration);

        job.setJarByClass(SerDriver.class);
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Access.class);

        String intput="inputDir/access.log";
        String output="output";

        FileInputFormat.setInputPaths(job,new Path(intput));
        FileOutputFormat.setOutputPath(job,new Path(output));

        job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class WcMapper extends Mapper<LongWritable, Text,Text,Access> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(value==null)
                return;

            String[] array= value.toString().split("\t");
            String phone=array[1];
            int total=Integer.parseInt(array[4]);
            int up=Integer.parseInt(array[4]);
            int down=Integer.parseInt(array[5]);

            context.write(new Text(phone),new Access(phone,up,down,total));
        }
    }

    public static class WcReduce extends Reducer<Text, Access, NullWritable,Access> {

        @Override
        protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
            int up=0;
            int down=0;
            int total=0;
            for (Access access:values) {
                down+=access.getDown();
                up+=access.getUp();
                total+=access.getTotal();
            }
            context.write(null,new Access(key.toString(),up,down,total));
        }
    }
}
