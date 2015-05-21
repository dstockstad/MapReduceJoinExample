package com.amazonaws.dags.hadoop.examples.join.inputformats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DBPediaLogValue implements Writable {
  private String www;
  private String category;
  private String flag;
  
  public String getWww() {
    return www;
  }

  public void setWww(String www) {
    this.www = www;
  }

  public String getCategory() {
    return category;
  }

  public void setCategory(String category) {
    this.category = category;
  }

  public String getFlag() {
    return flag;
  }

  public void setFlag(String flag) {
    this.flag = flag;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    www = in.readUTF();
    category = in.readUTF();
    flag = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(www);
    out.writeUTF(category);
    out.writeUTF(flag);
  }
  
  @Override
  public String toString() {
    return www + " " + category + " " + flag;
  }
}
