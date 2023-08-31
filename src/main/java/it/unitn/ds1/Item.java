package it.unitn.ds1;

// Item class definition
public class Item {
  public String value;
  public int version;
  public boolean lock; 

  public Item(String value, int version, boolean lock){
    this.value = value;
    this.version = version;
    this.lock = lock;
  }

  public Item(Item copy){
    this.value = copy.value;
    this.version = copy.version;
    this.lock = copy.lock;
  }
}