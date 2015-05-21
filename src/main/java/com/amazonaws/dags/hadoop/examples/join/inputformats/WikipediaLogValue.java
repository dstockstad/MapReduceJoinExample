package com.amazonaws.dags.hadoop.examples.join.inputformats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class WikipediaLogValue implements Writable {
  private String site;
  private long views;
  private long responseBytes;
  
  public String getSite() {
    return site;
  }

  public void setSite(String site) {
    this.site = site;
  }

  public long getViews() {
    return views;
  }

  public void setViews(long views) {
    this.views = views;
  }

  public long getResponseBytes() {
    return responseBytes;
  }

  public void setResponseBytes(long responseBytes) {
    this.responseBytes = responseBytes;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    site = in.readUTF();
    views = in.readLong();
    responseBytes = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(site);
    out.writeLong(views);
    out.writeLong(responseBytes);
  }
  
  @Override
  public String toString() {
    return site + " " + views + " " + responseBytes;
  }
}
