package com.amazonaws.dags.hadoop.examples.join.inputformats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class WikipediaLogKey implements Writable {
  private String page;
  
  
  public String getPage() {
    return page;
  }

  public void setPage(String page) {
    this.page = page;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    page = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(page);
  }
  
  @Override
  public String toString() {
    return this.page;
  }
}
