package com.ruoze.hadoop.inputformat;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeptWritable implements DBWritable, Writable {
    private String pName;
    private String pCode;
    private String deptCode;
    private String city;

    public DeptWritable(){

    }

    public DeptWritable(String pName,String pCode,String deptCode,String city){
        this.pName=pName;
        this.pCode=pCode;
        this.deptCode=deptCode;
        this.city=city;
    }
    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getpCode() {
        return pCode;
    }

    public void setpCode(String pCode) {
        this.pCode = pCode;
    }

    public String getDeptCode() {
        return deptCode;
    }

    public void setDeptCode(String deptCode) {
        this.deptCode = deptCode;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //out.writeUTF();

    }

    @Override
    public void readFields(DataInput in) throws IOException {


    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,this.pName);
        statement.setString(2,this.pCode);
        statement.setString(3,this.deptCode);
        statement.setString(4,this.city);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.pName=resultSet.getString(1);
        this.pCode=resultSet.getString(2);
        this.deptCode=resultSet.getString(3);
        this.city=resultSet.getString(4);
    }
}
