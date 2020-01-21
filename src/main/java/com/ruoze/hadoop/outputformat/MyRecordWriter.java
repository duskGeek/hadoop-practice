package com.ruoze.hadoop.outputformat;

import org.apache.commons.io.FileSystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream pkOut;
    FSDataOutputStream xingxingOut;
    FSDataOutputStream otherOut;
    FileSystem fs;

    public MyRecordWriter(TaskAttemptContext context){
        Configuration conf=context.getConfiguration();
        try {
            this.fs=FileSystem.get(conf);
            pkOut=fs.create(new Path("out/pk.log"));
            xingxingOut=fs.create(new Path("out/xingxing.log"));
            otherOut=fs.create(new Path("out/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String keyStr=key.toString();
        if (keyStr.startsWith("PK")){
            pkOut.writeUTF(keyStr+"\r\n");
        }else if(keyStr.startsWith("xingxing")){
            xingxingOut.writeUTF(keyStr+"\r\n");
        }else{
            otherOut.writeUTF(keyStr+"\r\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(pkOut);
        IOUtils.closeStream(xingxingOut);
        IOUtils.closeStream(otherOut);
    }
}
