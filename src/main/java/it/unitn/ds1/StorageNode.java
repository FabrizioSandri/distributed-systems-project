package it.unitn.ds1;

import it.unitn.ds1.ClientNode.ErrorMsg;
import it.unitn.ds1.ClientNode.GetRequestMsg;
import it.unitn.ds1.ClientNode.GetResponseMsg;
import it.unitn.ds1.ClientNode.UpdateRequestMsg;
import scala.concurrent.duration.Duration;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

  private Map<Integer, List<Item>> quorum;

  /*-- StorageNode constructors --------------------------------------------- */
  public StorageNode(int id) {
    this.id = id;
    this.requestId = 0;

    requestSender = new HashMap<>();
    storageNodes = new HashMap<>();
    storage = new HashMap<>();
    quorum = new HashMap<>();
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
    public final boolean readReq;
    public ReadResponse(int key, String value, int version, int requestId, boolean readReq) {
      this.key = key;
      this.item = new Item(value, version);
      this.requestId = requestId;
      this.readReq = readReq;
    }
  }
  public static class WriteMsg implements Serializable { // From storage node to storage node
    public final int key;
    public final Item item;

    public WriteMsg(int key, Item item) {
      this.key = key;
      this.item=item;
    }
  }

  // Used to specify that a given request id has timed out
  public static class TimeoutMsg implements Serializable {
    public final int requestId;
    public final int minQuorumSize;

    public TimeoutMsg(int requestId, int minQuorumSize) {
      this.requestId = requestId;      
      this.minQuorumSize = minQuorumSize;      
    }
  }

  /*-- Message handlers ----------------------------------------------------- */
  private void onJoinGroupMsg(JoinGroupMsg msg) {
    for (int storageNodeId: msg.storageNodes.keySet()) {
      this.storageNodes.put(storageNodeId, msg.storageNodes.get(storageNodeId));
    }
    log("Joined the storage network");
  }

  private void onReadRequest(ReadRequest msg) {
    int version = -1;
    String value = "";

    // Check if the storage contains the requested item
    if (storage.containsKey(msg.key)) {
      version = storage.get(msg.key).version;
      value = storage.get(msg.key).value;
    }

    // Send the item as a response to the request
    ReadResponse res = new ReadResponse(msg.key, value, version, msg.requestId, true);
    getSender().tell(res, getSelf());

  }

  private void onReadResponse(ReadResponse msg) {
    int minQuorumNumber=R;
    if (!msg.readReq){
        minQuorumNumber=W;
    }

    int requestId = msg.requestId;

    // if this is the first response create the list to hold the quorum
    if (!quorum.containsKey(requestId)) {
      quorum.put(requestId, new ArrayList<>());
    }

    Item readResponse = new Item(msg.item.value, msg.item.version);
    quorum.get(requestId).add(readResponse);

    // As soon as R replies arrive, send the response to the client that
    // originated that request id. If size > R then discard the responses
    // because the response has already been sent
    if (quorum.get(requestId).size() == minQuorumNumber ){

      Item mostRecentItem = quorum.get(requestId).get(0);
      int mostRecentVersion = quorum.get(requestId).get(0).version;
      
      // find the item with the highest version
      for (Item it : quorum.get(requestId)){
        if (it.version > mostRecentVersion){
          mostRecentItem = it;
        }
      }

      // send back the response
      // TD change GetResponseMsg in ResponseMsg since a response to the client is the same for Get or Update
      GetResponseMsg getResponse = new GetResponseMsg(mostRecentItem);
      requestSender.get(requestId).tell(getResponse, getSelf());
      
      if (!msg.readReq){  // if the case of a write
        List<Integer> nodesToBeContacted = findNodesForKey(msg.key);
        this.requestId++;
        WriteMsg writeMSg = new WriteMsg(msg.key, mostRecentItem);
        for (int storageNodeId : nodesToBeContacted){
          storageNodes.get(storageNodeId).tell(writeMSg, getSelf());
        }
      }
    }
  }

  private void onGetRequest(GetRequestMsg msg) {

    // schedule the timeout after T seconds
    getContext().system().scheduler().scheduleOnce(
      Duration.create(T * 1000, TimeUnit.MILLISECONDS),     // how frequently generate them
      getSelf(),                                            // destination actor reference
      new TimeoutMsg(this.requestId, R),                    // the message to send
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
      new TimeoutMsg(this.requestId, W),                    // the message to send
      getContext().system().dispatcher(),                   // system dispatcher
      getSelf()                                             // source of the message (myself)
    );

    // Contact the N nodes
    List<Integer> nodesToBeContacted = findNodesForKey(msg.key);

    ReadRequest readMsg = new ReadRequest(msg.key, this.requestId, RequestType.WRITE);
    requestSender.put(this.requestId, getSender());

    this.requestId++; // increase the request id for following requests

    for (int storageNodeId : nodesToBeContacted){
      storageNodes.get(storageNodeId).tell(readMsg, getSelf());
    }
  }

  private void onWriteMsg(WriteMsg msg){
    
    storage.put(msg.key, msg.item);
    //TD unlock the item
  }


  private void onTimeout(TimeoutMsg msg) {
    if (quorum.get(msg.requestId).size() < msg.minQuorumSize){  // send an error message to the client that originated the request
      ErrorMsg error = new ErrorMsg("The request with id " + requestId + " timed out.");
      requestSender.get(msg.requestId).tell(error, getSender());
    }else{
      // TODO: it's possible that in the meanwhile all the messages arrived
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
        .match(WriteMsg.class, this::onWriteMsg)
        .match(TimeoutMsg.class, this::onTimeout)
        .build();
  }

}
