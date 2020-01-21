package com.ruoze.hadoop.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Dept implements Writable {

    private String deptNo;
    private String deptName;
    private String city;

    public Dept(){

    }

    public Dept(String deptNo, String deptName, String city ){
        this.deptNo=deptNo;
        this.deptName=deptName;
        this.city=city;
    }

    public String getDeptNo() {
        return deptNo;
    }

    public void setDeptNo(String deptNo) {
        this.deptNo = deptNo;
    }

    public String getDeptName() {
        return deptName;
    }

    public void setDeptName(String deptName) {
        this.deptName = deptName;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.deptNo);
        out.writeUTF(this.deptName);
        out.writeUTF(this.city);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.deptNo=in.readUTF();
        this.deptName=in.readUTF();
        this.city=in.readUTF();
    }

    @Override
    public String toString() {
        return this.deptNo+"\t"+this.deptName+"\t"+this.city;
    }
}
