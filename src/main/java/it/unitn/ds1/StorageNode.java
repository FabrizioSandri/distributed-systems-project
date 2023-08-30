package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.ClientNode.ErrorMsg;
import it.unitn.ds1.ClientNode.GetRequestMsg;
import it.unitn.ds1.ClientNode.GetResponseMsg;
import it.unitn.ds1.ClientNode.UpdateRequestMsg;
import it.unitn.ds1.ClientNode.UpdateResponseMsg;
import scala.concurrent.duration.Duration;

public class StorageNode extends AbstractActor {

  final static int N = 3;
  final static int R = 2;
  final static int W = 2;
  final static int T = 2;

  public enum RequestType { READ, WRITE }

  // The set of all the storage node composing the storage network. This map
  // associates the node id to the corresponding ActorRef reference
  private Map<Integer, ActorRef> storageNodes;

  // This is the storage of each node in the storage network, associating keys
  // to items(value and version)
  private Map<Integer, Item> storage;

  private int id; // the node id

  // This variable holds thr count of incoming client requests. It serves the
  // purpose of distinguishing between different requests. Whenever a client
  // communicates with this storage node for a 'get' or 'update' request, the
  // requestId value is incremented.
  // requestSender then maps request IDs to their originating clients.
  private int requestId;
  private Map<Integer, ActorRef> requestSender;
  private Map<Integer, Boolean> fulfilled;
  private Map<Integer, ActorRef> locker;
  private Map<Integer, List<Item>> quorum;

  // Used by the coordinator to retrieve the value to be written in a write
  // request given the request id  
  private Map<Integer, String> toWrite;

  /*-- StorageNode constructors --------------------------------------------- */
  public StorageNode(int id) {
    this.id = id;
    this.requestId = 0;
    
    requestSender = new HashMap<>();
    storageNodes = new HashMap<>();
    storage = new HashMap<>();
    quorum = new HashMap<>();
    fulfilled = new HashMap<>();
    toWrite = new HashMap<>();
    locker = new HashMap<>();
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

  // Request for an item sent from a storage node to storage node
  public static class ReadRequest implements Serializable {
    public final int key;
    public final int requestId;
    public final RequestType reqType;

    public ReadRequest(int key, int requestId, RequestType reqType) {
      this.key = key;
      this.requestId = requestId;
      this.reqType=reqType;
    }
  }

  // Response for an item sent from a storage node to storage node that asked it
  public static class ReadResponse implements Serializable {
    public final int key;
    public final Item item;
    public final int requestId;
    public final RequestType reqType;

    public ReadResponse(int key, String value, int version, int requestId, RequestType reqType) {
      this.key = key;
      this.item = new Item(value, version, false);
      this.requestId = requestId;
      this.reqType = reqType;
    }
  }

  // Request write for an item sent from a storage node to storage node
  public static class WriteRequestMsg implements Serializable {
    public final int key;
    public final int requestId;
    public final ActorRef clientNode;

    public WriteRequestMsg(int key, int requestId, ActorRef clientNode) {
      this.key = key;
      this.requestId = requestId;
      this.clientNode= clientNode;
    }
  }

  public static class WriteResponseMsg implements Serializable {
    public final int version;
    public final int key;
    public final int requestId;
  
    public WriteResponseMsg(int version, int key, int requestId) {
      this.version = version;
      this.key = key;
      this.requestId = requestId;
    }
  }

  // Used to specify that a given request id has timed out
  public static class TimeoutMsg implements Serializable {
    public final int requestId;
    public final int minQuorumSize;
    public final int key;

    public TimeoutMsg(int requestId, int minQuorumSize, int key) {
      this.requestId = requestId;      
      this.minQuorumSize = minQuorumSize;     
      this.key = key;
    }
  }

  public static class RealeaseLockMsg implements Serializable {
    public final ActorRef requester;
    public final int key;

    public RealeaseLockMsg(ActorRef requester, int key) {
      this.requester = requester;   
      this.key = key;
    }
  }    

  /*-- Message handlers ----------------------------------------------------- */
  private void onJoinGroupMsg(JoinGroupMsg msg) {
    for (int storageNodeId: msg.storageNodes.keySet()) {
      this.storageNodes.put(storageNodeId, msg.storageNodes.get(storageNodeId));
    }
    log("Joined the storage network");
  }
  
  private void onWriteRequest(WriteRequestMsg msg) {
    int version = 0;
    
    if (storage.containsKey(msg.key) && !storage.get(msg.key).lock){ // check if item already exists in the storage and if someone is already using it
      storage.get(msg.key).lock = true; 
      
      // save the client that locked the item, also signaling that there is already someone that is working on the key
      locker.put(msg.key, msg.clientNode);  

      // Check if the storage contains the requested item
      version = storage.get(msg.key).version;

      // Send the item as a response to the request
      WriteResponseMsg res = new WriteResponseMsg(version, msg.key, msg.requestId);
      getSender().tell(res, getSelf());
    
    }else if(!locker.containsKey(msg.key)){ // check if a creation of the item is taking place, if not create then the item

      // save the client that locked the item, signaling that there is already someone that is working on the item
      locker.put(msg.key, msg.clientNode); 
    
      // Send the item as a response to the request
      WriteResponseMsg res = new WriteResponseMsg(version, msg.key, msg.requestId);
      getSender().tell(res, getSelf());
    }
  }

  private void onWriteResponse(WriteResponseMsg msg) {
    int requestId = msg.requestId;

    // if this is the first response create the list to hold the quorum
    if (!quorum.containsKey(requestId)) {
      quorum.put(requestId, new ArrayList<>());
    }

    Item readResponse = new Item(null, msg.version, false);
    quorum.get(requestId).add(readResponse);

    // As soon as W replies arrive, send the response to the client that
    // originated that request id. If size > W then discard the responses
    // because the response has already been sent
    if (quorum.get(requestId).size() == W && fulfilled.containsKey(requestId) == false){
      
      // The request has been fulfilled and thus 
      fulfilled.put(requestId, true); 
      
      int mostRecentVersion = quorum.get(requestId).get(0).version;
      
      // find the item with the highest version
      for (Item it : quorum.get(requestId)){
        if (it.version > mostRecentVersion){
            mostRecentVersion = it.version;
        }
      }

      // send back the response        
      List<Integer> nodesToBeContacted = findNodesForKey(msg.key);

      // create updated item and send it to the client and the other nodes
      Item newItem =  new Item(toWrite.get(requestId), mostRecentVersion+1, false);
      UpdateResponseMsg updateResponse = new UpdateResponseMsg(msg.key, newItem);
      requestSender.get(requestId).tell(updateResponse, getSelf());

      for (int storageNodeId : nodesToBeContacted){
        storageNodes.get(storageNodeId).tell(updateResponse, getSelf());
      }
    }
    // TODO: for efficiency reasons in this part it's possible to check if
    // quorum.get(requestId).size() == N and then remove the key from quorum
  }


  private void onReadRequest(ReadRequest msg) {
    int version = 0;
    String value = "";
      
    // Check if the storage contains the requested item
    if (storage.containsKey(msg.key) && !storage.get(msg.key).lock) { // check if the system is updating the item
      version = storage.get(msg.key).version;
      value = storage.get(msg.key).value;

      // Send the item as a response to the request
      ReadResponse res = new ReadResponse(msg.key, value, version, msg.requestId, msg.reqType);
      getSender().tell(res, getSelf());
    
    }
  }

  private void onReadResponse(ReadResponse msg) {

    int requestId = msg.requestId;

    // if this is the first response create the list to hold the quorum
    if (!quorum.containsKey(requestId)) {
      quorum.put(requestId, new ArrayList<>());
    }

    Item readResponse = new Item(msg.item.value, msg.item.version, false);
    quorum.get(requestId).add(readResponse);

    // As soon as R replies arrive, send the response to the client that
    // originated that request id. If size > R then discard the responses
    // because the response has already been sent
    if (quorum.get(requestId).size() == R && fulfilled.containsKey(requestId) == false){
      
      // The request has been fulfilled and thus 
      fulfilled.put(requestId, true); 
      
      Item mostRecentItem = quorum.get(requestId).get(0);
      int mostRecentVersion = quorum.get(requestId).get(0).version;
      
      // find the item with the highest version
      for (Item it : quorum.get(requestId)){
        if (it.version > mostRecentVersion){
          mostRecentItem = it;
          mostRecentVersion = it.version;
        }
      }

      // send back the response
      GetResponseMsg getResponse = new GetResponseMsg(mostRecentItem);
      requestSender.get(requestId).tell(getResponse, getSelf());
    }
    // TODO: for efficiency reasons in this part it's possible to check if
    // quorum.get(requestId).size() == N and then remove the key from quorum
  }

  private void onGetRequest(GetRequestMsg msg) {

    // schedule the timeout after T seconds
    getContext().system().scheduler().scheduleOnce(
      Duration.create(T * 1000, TimeUnit.MILLISECONDS),     // how frequently generate them
      getSelf(),                                            // destination actor reference
      new TimeoutMsg(this.requestId, R, msg.key),           // the message to send
      getContext().system().dispatcher(),                   // system dispatcher
      getSelf()                                             // source of the message (myself)
    );

    // Contact the N nodes
    List<Integer> nodesToBeContacted = findNodesForKey(msg.key);
    ReadRequest readMsg = new ReadRequest(msg.key, this.requestId, RequestType.READ);
    requestSender.put(this.requestId, getSender());

    this.requestId++; // increase the request id for following requests
    for (int storageNodeId : nodesToBeContacted){
      storageNodes.get(storageNodeId).tell(readMsg, getSelf());
    }

  }

  private void onUpdateRequest(UpdateRequestMsg msg) {

    // schedule the timeout after T seconds
    getContext().system().scheduler().scheduleOnce(
      Duration.create(T * 1000, TimeUnit.MILLISECONDS),     // how frequently generate them
      getSelf(),                                            // destination actor reference
      new TimeoutMsg(this.requestId, W, msg.key),           // the message to send
      getContext().system().dispatcher(),                   // system dispatcher
      getSelf()                                             // source of the message (myself)
    );

    // Save the item to be written for this request id. Used then by this node
    // once the write quorum has been reached
    toWrite.put(requestId, msg.value);

    // Contact the N nodes
    List<Integer> nodesToBeContacted = findNodesForKey(msg.key);
    
    WriteRequestMsg readMsg = new WriteRequestMsg(msg.key, this.requestId, getSender());
    requestSender.put(this.requestId, getSender());

    this.requestId++; // increase the request id for following requests
    for (int storageNodeId : nodesToBeContacted){
      storageNodes.get(storageNodeId).tell(readMsg, getSelf());
    }
  }

  private void onUpdateResponse(UpdateResponseMsg msg){
    storage.put(msg.key, new Item(msg.item.value, msg.item.version, msg.item.lock));
    
    // unlock the item to state that the creation happened or 
    //the updating from a client finished (setted in onWriteRequest to avoid w-w conflint during creation or ordinary update)
    locker.remove(msg.key);
    log("The item with key " + msg.key + " has been updated to '" + msg.item.value + "' (v" + msg.item.version + ")");
  }

  private void onTimeout(TimeoutMsg msg) {    
    fulfilled.put(msg.requestId, true);
    List<Integer> nodesToBeContacted = findNodesForKey(msg.key);

    // send an error message to the client that originated the request. Check
    // again that the quorum in the meanwhile has not been reached
    if (quorum.get(msg.requestId).size() < msg.minQuorumSize){  
      ErrorMsg error = new ErrorMsg("The request with id " + requestId + " timed out.");
      requestSender.get(msg.requestId).tell(error, getSender());
      
      //send the message to all the node that have been contacted to release the locks enabled during the write request 
      RealeaseLockMsg releaseLockMsg = new RealeaseLockMsg(requestSender.get(msg.requestId), msg.key);
      for (int storageNodeId : nodesToBeContacted){
        storageNodes.get(storageNodeId).tell(releaseLockMsg, getSelf());
      }
    }
  }

  private void onReleaseLock(RealeaseLockMsg msg){
    // permit to unlock the items only to the coordinator that started an update that timed out 
    // the lock request was tracked using the client node reference since an actor can 
    // make a request at the time 
    if (locker.containsKey(msg.key) && locker.get(msg.key) == msg.requester){ 
      locker.remove(msg.key);

      //need to unlock the item lock if the node manage to get the lock on it even if the request timed out 
      if (storage.containsKey(msg.key)){
        storage.get(msg.key).lock = false;
      }
    }
  }

  /*-- Auxiliary functions -------------------------------------------------- */

  // Find the N nodes that has to be contacted for a given key
  List<Integer> findNodesForKey(int key){

    List<Integer> keySet = new ArrayList<>(storageNodes.keySet());
    List<Integer> nodesToBeContacted = new ArrayList<>();
    Collections.sort(keySet);

    int n=0;
    for(int i=0; i<storageNodes.size() && n<N; i++){ 
      if (keySet.get(i) >= key){
        nodesToBeContacted.add(keySet.get(i));
        n++;
      }
    }

    // take the remaining items from the beginning of the ring(modulo)
    if (nodesToBeContacted.size() < N){ 
      for (int i=0; i<N-n; i++){
        nodesToBeContacted.add(keySet.get(i));
      }
    }

    return nodesToBeContacted;
  }

  // log a given message while also printing the storage node id
  void log(String message){
    System.out.println("[S" + id + "] " + message);
  }

  // Mapping between the received message types and this actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onJoinGroupMsg)
        .match(ReadRequest.class, this::onReadRequest)
        .match(UpdateRequestMsg.class, this::onUpdateRequest)
        .match(GetRequestMsg.class, this::onGetRequest)
        .match(ReadResponse.class, this::onReadResponse)
        .match(WriteRequestMsg.class,this::onWriteRequest)
        .match(WriteResponseMsg.class, this::onWriteResponse)
        .match(UpdateResponseMsg.class, this::onUpdateResponse)
        .match(TimeoutMsg.class, this::onTimeout)
        .match(RealeaseLockMsg.class, this::onReleaseLock)
        .build();
  }

}
