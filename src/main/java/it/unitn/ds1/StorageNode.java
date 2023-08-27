package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageNode extends AbstractActor{

  final static int N = 3;

  private Map<Integer, String> storage;
  private Map<Integer, Integer> storageVersions;
  private Map<Integer, ActorRef> storageNodes;

  private int id;

  /*-- StorageNode constructors --------------------------------------------- */
  public StorageNode(int id) {
    this.id = id;

    storageNodes = new HashMap<Integer, ActorRef>();
    storage = new HashMap<Integer, String>();
    storageVersions = new HashMap<Integer, Integer>();
  }

  static public Props props(int id) {
    return Props.create(StorageNode.class, () -> new StorageNode(id));
  }


  /*-- Message classes ------------------------------------------------------ */
  public static class JoinGroupMsg implements Serializable {
    public final Map<Integer, ActorRef> storageNodes;
    public JoinGroupMsg(Map<Integer, ActorRef> storageNodes) {
      this.storageNodes = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(storageNodes));
    }
  }

  public static class ReadMsg implements Serializable { // From storage node to storage node
    public final int key;
    public ReadMsg(int key){
      this.key = key;
    }
  }

  public static class WriteMsg implements Serializable { // From storage node to storage node
    public final int key;
    public final String value;
    public WriteMsg(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class GetMsg implements Serializable {  // From client to storage node
    public final int key;
    public GetMsg(int key){
      this.key = key;
    }
  }

  public static class UpdateMsg implements Serializable { // From client to storage node
    public final int key;
    public final String value;
    public UpdateMsg(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class ReadResponse implements Serializable {
    public final int key;
    public final String value;
    public final int version;
    public ReadResponse(int key, String value, int version){
      this.key = key;
      this.value = value;
      this.version = version;
    }
  }


  /*-- Message handlers ----------------------------------------------------- */
  private void onJoinGroupMsg(JoinGroupMsg msg) {
    for (int storageNodeId: msg.storageNodes.keySet()) {
      if (storageNodeId != this.id) { // copy all storage nodes except for self
        this.storageNodes.put(storageNodeId, msg.storageNodes.get(storageNodeId));
      }
    }
    System.out.println("[" + id + "] Joining the storage network");
  }

  private void onReadMsg(ReadMsg msg) {
    int version = -1;
    String value = "";

    // Check if the storage contains the requested key
    if (storageVersions.containsKey(msg.key) && storage.containsKey(msg.key)){
      version = storageVersions.get(msg.key);
      value = storage.get(msg.key);
    }

    // Send the item as a response to the request
    ReadResponse res = new ReadResponse(msg.key, value, version);
    getSender().tell(res, getSender());

  }


  private void onGetMsg(GetMsg msg){

    // Contact the N nodes
    List<Integer> nodesToBeContacted = findNodesForKey(msg.key);
    ReadMsg readMsg = new ReadMsg(msg.key);

    for (int storageNodeId : nodesToBeContacted){
      storageNodes.get(storageNodeId).tell(readMsg, getSender());
    }

  }
  
  private void onUpdateMsg(UpdateMsg msg){

    // Contact the N nodes
    List<Integer> nodesToBeContacted = findNodesForKey(msg.key);

    // TODO: implement


  }

  /*-- Auxiliary functions -------------------------------------------------- */

  // Find the N nodes that has to be contacted for a given key
  List<Integer> findNodesForKey(int key){

    List<Integer> keySet = new ArrayList<>(storageNodes.keySet());
    List<Integer> nodesToBeContacted = new ArrayList<>();
    Collections.sort(keySet);

    for(int i=0, n=0; i<storageNodes.size() && n<N; i++){ 
      if (keySet.get(i) >= key){
        nodesToBeContacted.add(keySet.get(i));
        n++;
      }
    }

    // take the remaining items from the beginning of the ring(modulo)
    if (nodesToBeContacted.size() < N){ 
      for (int i=0; i<N-nodesToBeContacted.size(); i++){
        nodesToBeContacted.add(keySet.get(i));
      }
    }

    return nodesToBeContacted;
  }


  // Mapping between the received message types and this actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
    .match(JoinGroupMsg.class,  this::onJoinGroupMsg)
    .match(ReadMsg.class,  this::onReadMsg)
    .match(UpdateMsg.class,  this::onUpdateMsg)
    .match(GetMsg.class,  this::onGetMsg)
    .build();
  }
  
}
