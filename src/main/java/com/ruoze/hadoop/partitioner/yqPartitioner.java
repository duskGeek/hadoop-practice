package com.ruoze.hadoop.partitioner;

import com.ruoze.hadoop.ser.Access;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class yqPartitioner extends Partitioner<Text, Access> {

    @Override
    public int getPartition(Text text, Access access, int numPartitions) {
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
