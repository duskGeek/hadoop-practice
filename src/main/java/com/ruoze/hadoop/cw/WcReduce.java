package com.ruoze.hadoop.cw;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WcReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int valSum=0;
        for (IntWritable val:values) {
            valSum+=val.get();
        }
        context.write(key,new IntWritable(valSum));

    }
}
