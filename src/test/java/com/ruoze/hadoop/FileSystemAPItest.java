package com.ruoze.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class FileSystemAPItest {

    FileSystem fs;

    @Before
    public void setUp()throws Exception{
        URI uri = new URI("hdfs://yqdata000:8020");
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname","true");
        configuration.set("dfs.replication","1");
        fs=FileSystem.get(uri,configuration,"hadoop");
    }

    @After
    public void setTearDown()throws Exception{
        if(fs!=null){
            fs.close();
        }
    }

    @Test
    public void mkdir() throws Exception{
        Path path = new Path("/20200113");
        fs.mkdirs(path);
    }

    @Test
    public void copyFromLocal() throws IOException {
        Path srcPath = new Path("inputDir/car");
        Path dstPath = new Path("/20200112/");
        fs.copyFromLocalFile(srcPath,dstPath);

    }

    @Test
    public void copyToLocal() throws IOException {
        Path srcPath = new Path("/hello.txt");
        Path dstPath = new Path("output/");
        fs.copyToLocalFile(srcPath,dstPath);
    }

    @Test
    public void rename()throws IOException{
        Path path = new Path("/test1.txt");
        Path path1 = new Path("/20200112/test1.txt");
        fs.rename(path,path1);
    }

}
