package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class StorageNode extends AbstractActor{

  private HashMap<Integer, String> storage;
  private HashMap<Integer, Integer> storageVersions;

  private List<ActorRef> storageNodes;

  private int id;
  

  /*-- StorageNode constructors --------------------------------------------- */
  public StorageNode(int id) {
    this.id = id;

    storageNodes = new ArrayList<>();
    storage = new HashMap<Integer, String>();
    storageVersions = new HashMap<Integer, Integer>();
  }

  static public Props props(int id) {
    return Props.create(StorageNode.class, () -> new StorageNode(id));
  }


  /*-- Message classes ------------------------------------------------------ */
  public static class JoinGroupMsg implements Serializable {
    public final List<ActorRef> storageNodes;   // an array of storage nodes
    public JoinGroupMsg(List<ActorRef> storageNodes) {
      this.storageNodes = Collections.unmodifiableList(new ArrayList<ActorRef>(storageNodes));
    }
  }


  /*-- Message handlers ----------------------------------------------------- */
  private void onJoinGroupMsg(JoinGroupMsg msg) {
    for (ActorRef s: msg.storageNodes) {
      if (!s.equals(getSelf())) { // copy all storge nodes except for self
        this.storageNodes.add(s);
      }
    }
    System.out.println("[" + id + "] Joining the storage network");
  }


  // Mapping between the received message types and this actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(JoinGroupMsg.class,  this::onJoinGroupMsg)
      .build();
  }
  
}
