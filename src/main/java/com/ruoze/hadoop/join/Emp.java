package com.ruoze.hadoop.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Emp implements Writable {

    private String empNo;
    private String empName;
    private String deptNo;
    private String deptName;
    private int flag;

    public Emp(){

    }

    public Emp( String empNo, String empName,String deptNo,String deptName,int flag ){
        this.empNo=empNo;
        this.empName=empName;
        this.deptNo=deptNo;
        this.deptName=deptName;
        this.flag=flag;
    }

    public String getEmpNo() {
        return empNo;
    }

    public void setEmpNo(String empNo) {
        this.empNo = empNo;
    }

    public String getEmpName() {
        return empName;
    }

    public void setEmpName(String empName) {
        this.empName = empName;
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

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.empNo);
        out.writeUTF(this.empName);
        out.writeUTF(this.deptNo);
        out.writeUTF(this.deptName);
        out.writeInt(this.flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.empNo=in.readUTF();
        this.empName=in.readUTF();
        this.deptNo=in.readUTF();
        this.deptName=in.readUTF();
        this.flag=in.readInt();
    }

    @Override
    public String toString() {
        return this.empNo+"\t"+this.empName+"\t"+this.deptNo+"\t"+this.deptName+"\t";
    }
}
