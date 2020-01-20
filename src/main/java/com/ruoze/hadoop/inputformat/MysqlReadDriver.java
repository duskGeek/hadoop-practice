package com.ruoze.hadoop.inputformat;

import com.ruoze.hadoop.ser.Access;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MysqlReadDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration configuration=new Configuration();
        ToolRunner.run(configuration,new MysqlReadDriver(),args);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {

            Configuration configuration=super.getConf();
            DBConfiguration.configureDB(configuration,"com.mysql.jdbc.Driver",
                    "jdbc:mysql://yqdata000:3306/yqdata","root","499266134");

            Job job=job = Job.getInstance(configuration);

            job.setJarByClass(MysqlReadDriver.class);
            job.setMapperClass(WcMapper.class);

            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(DeptWritable.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(DeptWritable.class);

            job.setInputFormatClass(DBInputFormat.class);

            String[] fildNames=new String[]{"p_name","p_code","dept_code","city"};
            DBInputFormat.setInput(job,DeptWritable.class,"dept",
                    "","",fildNames);

            String output="out";
            FileOutputFormat.setOutputPath(job,new Path(output));

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }


        return 0;
    }

    public static class WcMapper extends Mapper<LongWritable, DeptWritable,NullWritable,DeptWritable> {

        @Override
        protected void map(LongWritable key, DeptWritable value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(),value);
        }
    }
}
