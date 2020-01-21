package com.ruoze.hadoop.join;

import com.ruoze.hadoop.ser.Access;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinDriver {

    public static void main(String[] args) {

        try {

        Configuration configuration=new Configuration();
        Job job=job = Job.getInstance(configuration);

        job.setJarByClass(JoinDriver.class);
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Emp.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        String intput="inputDir/join/";
        String output="output";

        FileInputFormat.setInputPaths(job,new Path(intput));
        FileOutputFormat.setOutputPath(job,new Path(output));

        job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class WcMapper extends Mapper<LongWritable, Text,Text,Emp> {

        String fileName="";
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit inputSplit=(FileSplit)context.getInputSplit();
            Path path=inputSplit.getPath();
            fileName=path.getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(value==null)
                return;


            String[] array= value.toString().split("\t");

            if(fileName.contains("emp")){
                if(array==null||array.length<8){
                    return;
                }
                 String empNo=array[0];
                 String empName=array[1];
                 String deptNo=array[7];
                context.write(new Text(deptNo),new Emp(empNo,empName,deptNo,"",1));
            }else{
                if(array==null||array.length<3){
                    return;
                }
                String deptNo=array[0];
                String deptName=array[1];
                context.write(new Text(deptNo),new Emp("","",deptNo,deptName,2));
            }
        }
    }

    public static class WcReduce extends Reducer<Text, Emp, Emp,NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Emp> values, Context context) throws IOException, InterruptedException {
           List<Emp> outList=new ArrayList<Emp>();
           String deptName="";

            for (Emp emp:values) {
                if(emp.getFlag()==1){
                    Emp tmpEmp=new Emp(emp.getEmpNo(),emp.getEmpName(),emp.getDeptNo(),"",1);
                    outList.add(tmpEmp);
                }else{
                    deptName=emp.getDeptName();
                }
            }

            for (Emp emp:outList) {
                emp.setDeptName(deptName);
                context.write(emp,NullWritable.get());
            }

        }
    }
}
