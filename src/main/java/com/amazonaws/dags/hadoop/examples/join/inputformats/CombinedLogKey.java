package com.amazonaws.dags.hadoop.examples.join.inputformats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CombinedLogKey implements WritableComparable<CombinedLogKey> {
  private String page;
  private int dataSetNumber;
  
  public int getDataSetNumber() {
    return dataSetNumber;
  }

  public void setDataSetNumber(int dataSetNumber) {
    this.dataSetNumber = dataSetNumber;
  }

  public String getPage() {
    return page;
  }

  public void setPage(String page) {
    this.page = page;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    page = in.readUTF();
    dataSetNumber = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(page);
    out.writeInt(dataSetNumber);
  }
  
  @Override
  public String toString() {
    return this.page;
  }

  @Override
  public int compareTo(CombinedLogKey o) {
    int result = page.compareTo(o.getPage());
    
    if (result == 0) {
      result = Integer.compare(dataSetNumber, o.getDataSetNumber());
    }
    return result;
  }
}
