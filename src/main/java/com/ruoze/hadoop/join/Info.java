package com.ruoze.hadoop.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * author：若泽数据-PK哥
 * 交流群：545916944
 */
public class Info implements Writable {

    private int empno;
    private String ename;
    private int deptno;
    private String dname;

    private int flag; //区分数据从哪里来的标志位

    public Info(){}

    public Info(int empno, String ename, int deptno, String dname){
        this.empno = empno;
        this.ename = ename;
        this.deptno = deptno;
        this.dname = dname;
    }

    public Info(int empno, String ename, int deptno, String dname, int flag){
        this(empno, ename,deptno,dname);
        this.flag = flag;
    }

    @Override
    public String toString() {
        return
                empno + "\t" + ename + "\t" + deptno +
                "\t" + dname ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(empno);
        out.writeUTF(ename);
        out.writeInt(deptno);
        out.writeUTF(dname);
        out.writeInt(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.empno = in.readInt();
        this.ename = in.readUTF();
        this.deptno = in.readInt();
        this.dname = in.readUTF();
        this.flag = in.readInt();
    }




    public int getEmpno() {
        return empno;
    }

    public void setEmpno(int empno) {
        this.empno = empno;
    }

    public String getEname() {
        return ename;
    }

    public void setEname(String ename) {
        this.ename = ename;
    }

    public int getDeptno() {
        return deptno;
    }

    public void setDeptno(int deptno) {
        this.deptno = deptno;
    }

    public String getDname() {
        return dname;
    }

    public void setDname(String dname) {
        this.dname = dname;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }
}
