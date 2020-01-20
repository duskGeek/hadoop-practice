package com.ruoze.hadoop.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortPartitioner extends Partitioner<Traffic,Text > {

    @Override
    public int getPartition(Traffic access,Text text,int numPartitions) {
        String phone=text.toString();
        if(phone.contains("138")){
            return 0;
        }if(phone.contains("186")){
            return 1;
        }else{
            return 2;
        }
    }
}
