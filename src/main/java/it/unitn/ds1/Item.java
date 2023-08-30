package it.unitn.ds1;

// Item class definition
public class Item {
  public String value;
  public int version;
  public boolean lock; 

  Item(String value, int version, boolean lock){
    this.value = value;
    this.version = version;
    this.lock = lock;
  }
}