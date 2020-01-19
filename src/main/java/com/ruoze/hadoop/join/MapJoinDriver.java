package com.ruoze.hadoop.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author：若泽数据-PK哥
 * 交流群：545916944
 */
public class MapJoinDriver {

    public static void main(String[] args) throws Exception {
        String input = "data/join/emp.txt";
        String output = "out";

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //FileUtils.deleteOutput(configuration, output);
        job.setJarByClass(MapJoinDriver.class);

        job.setMapperClass(MyMapper.class);
        job.setNumReduceTasks(0); // 不需要reduce

        job.addCacheFile(new URI("data/join/dept.txt"));

        job.setOutputKeyClass(Info.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }


    /**
     * 在Mapper中如何区分哪个数据是哪个表过来的....
     */
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Info> {

        Map<String, String> cache = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String path = context.getCacheFiles()[0].getPath().toString();

            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));

            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] splits = line.split("\t");
                cache.put(splits[0], splits[1]);  // 部门编号和部门名称
            }

            IOUtils.closeStream(reader);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            if (splits.length == 8) {
                int empno = Integer.parseInt(splits[0].trim());
                String ename = splits[1];
                int deptno = Integer.parseInt(splits[7].trim());

                Info info = new Info(empno, ename, deptno, cache.get(deptno+""));
                context.write(new IntWritable(deptno), info);
            }

        }
    }
}
