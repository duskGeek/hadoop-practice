package com.ruoze.hadoop.sort;

import com.ruoze.hadoop.partitioner.yqPartitioner;
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

public class AllSortDriver {

    public static void main(String[] args) {

        try {

        Configuration configuration=new Configuration();
        Job job=job = Job.getInstance(configuration);

        job.setJarByClass(AllSortDriver.class);
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReduce.class);

        job.setMapOutputKeyClass(Traffic.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(SortPartitioner.class);
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Traffic.class);

        String intput="inputDir/access.log";
        String output="output";

        FileInputFormat.setInputPaths(job,new Path(intput));
        FileOutputFormat.setOutputPath(job,new Path(output));

        job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class WcMapper extends Mapper<LongWritable, Text,Traffic,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(value==null)
                return;

            String[] array= value.toString().split("\t");
            String phone=array[1];
            int total=Integer.parseInt(array[4]);
            int up=Integer.parseInt(array[4]);
            int down=Integer.parseInt(array[5]);

            context.write(new Traffic(phone,up,down,total),new Text(phone));
        }
    }

    public static class WcReduce extends Reducer<Traffic,Text, Text,Traffic> {

        @Override
        protected void reduce(Traffic key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text phone:values) {
                context.write(phone,key);
            }
        }
    }
}
