package com.ruoze.hadoop.join;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * author：若泽数据-PK哥
 * 交流群：545916944
 */
public class ReduceJoinDriver {

    public static void main(String[] args) throws Exception{
        String input = "inputDir/join";
        String output = "out";

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //FileUtils.deleteOutput(configuration, output);
        job.setJarByClass(ReduceJoinDriver.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Info.class);

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

        String name;  // 标识数据从哪来的

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            name = split.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            if (name.contains("emp")) { // 处理emp的数据
                if(splits.length == 8) {
                    // 1
                    int empno = Integer.parseInt(splits[0].trim());
                    String ename = splits[1];
                    int deptno = Integer.parseInt(splits[7].trim());

                    Info info = new Info(empno, ename, deptno, "", 1); // 1:emp
                    context.write(new IntWritable(deptno), info);
                }
            } else { // dept
                int deptno = Integer.parseInt(splits[0].trim());
                Info info = new Info(0, "", deptno, splits[1], 2); // 2:dept
                context.write(new IntWritable(deptno), info);
            }

        }
    }


    // 真正的join操作是在此处完成的
    public static class MyReducer extends Reducer<IntWritable, Info, Info, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Info> values, Context context) throws IOException, InterruptedException {
            List<Info> infos = new ArrayList<>();
            String dname = "";
            for (Info info : values) {
                if (info.getFlag() == 1) { // emp
                    Info tmp = new Info();
                    tmp.setEmpno(info.getEmpno());
                    tmp.setEname(info.getEname());
                    tmp.setDeptno(info.getDeptno());
                    infos.add(tmp);
                } else
                if (info.getFlag() == 2) {  // dept
                    dname = info.getDname();
                }
            }

            for (Info bean : values) {
                bean.setDname(dname);
                context.write(bean, NullWritable.get());
            }

        }
    }

}
