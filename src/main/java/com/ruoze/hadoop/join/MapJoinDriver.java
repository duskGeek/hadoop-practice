package com.ruoze.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapJoinDriver {

    public static void main(String[] args) {

        try {

        Configuration configuration=new Configuration();
        Job job=job = Job.getInstance(configuration);

        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(WcMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Emp.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        String intput="inputDir/join/";
        String output="output";

        job.addCacheFile(new URI("inputDir/join/dept.txt"));

        FileInputFormat.setInputPaths(job,new Path(intput));
        FileOutputFormat.setOutputPath(job,new Path(output));

        job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class WcMapper extends Mapper<LongWritable, Text,Text,Emp> {

        Map<String, String> map = new HashMap<String, String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            URI[] uri = context.getCacheFiles();

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(String.valueOf(uri[0]))));

            String temp = "";
            while ((temp = bufferedReader.readLine()) != null) {
                String[] deptArr = temp.split("\t");
                map.put(deptArr[0], deptArr[1]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value == null)
                return;
            String[] array = value.toString().split("\t");
            if (array == null || array.length < 8) {
                return;
            }
            String empNo = array[0];
            String empName = array[1];
            String deptNo = array[7];

            context.write(new Text(deptNo), new Emp(empNo, empName, deptNo, map.get(deptNo), 1));


        }
    }

}
