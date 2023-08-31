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

  // The set of all the storage node composing the storage network. This map
  // associates the node id to the corresponding ActorRef reference
  private Map<Integer, ActorRef> storageNodes;

  // This is the storage of each node in the storage network, associating keys
  // to items(value and version)
  private Map<Integer, Item> storage;

  private int nodeId; // the node id

  // List of items keys that are necessary for a node that is joining the
  // network. The joining node must retrieve the most updated version of these
  // items
  List<Integer> necessaryItems;
  
  // The node employs this boolean variable to ignore any join messages it
  // receives once it has finished the joining process.
  boolean joined; 

  // This variable holds thr count of incoming client requests. It serves the
  // purpose of distinguishing between different requests. Whenever a client
  // communicates with this storage node for a 'get' or 'update' request, the
  // requestId value is incremented.
  // requestSender then maps request IDs to their originating clients.
  private int requestId;
  private Map<Integer, ActorRef> requestSender;
  private Map<Integer, Boolean> fulfilled;  // tells if a request id has been fulfilled

  // Maintains a record of the actor who initiated the item lock to prevent any
  // actor not responsible for the lock from unlocking it.
  private Map<Integer, ActorRef> lockedBy;

  // The items obtained from read requests for each request ID are stored in
  // this variable. This serves the purpose of both obtaining the most recent
  // version among the received items for a specific requestId and also for
  // determining if the quorum criteria are met for that requestId.
  private Map<Integer, List<Item>> quorum;

  // Used by the coordinator to retrieve the value to be written in a write
  // request given the request id  
  private Map<Integer, String> toWrite;

  // since the recovery operation uses a lot of join functions there is the
  // need to distringuish between a join and a recovery of the node
  private boolean recoveryMode;

  /*------------------------------------------------------------------------- */
  /*-- StorageNode constructors --------------------------------------------- */
  /*------------------------------------------------------------------------- */

  public StorageNode() {
    this.requestId = 0;
    this.joined = false;
    this.recoveryMode=false;

    requestSender = new HashMap<>();
    storageNodes = new HashMap<>();
    storage = new HashMap<>();
    quorum = new HashMap<>();
    fulfilled = new HashMap<>();
    toWrite = new HashMap<>();
    lockedBy = new HashMap<>();
  }

  static public Props props() {
    return Props.create(StorageNode.class, () -> new StorageNode());
  }
  
  /*------------------------------------------------------------------------- */
  /*-- Message classes - Item repartitioning -------------------------------- */
  /*------------------------------------------------------------------------- */

  // Join operation for a newly created node
  public static class JoinMsg implements Serializable {
    public final ActorRef bootstrappingPeer;
    public final int nodeId;
    public final boolean firstNode;

    public JoinMsg(int nodeId, ActorRef bootstrappingPeer, boolean firstNode) {
      this.bootstrappingPeer = bootstrappingPeer;
      this.nodeId = nodeId;
      this.firstNode = firstNode;
    }
  }

  // Requests the set of storage nodes in the storage network
  public static class GetSetOfNodesMsg implements Serializable { }

  // Response for the set of storage nodes in the storage network
  public static class SetOfNodesMsg implements Serializable {
    public final Map<Integer, ActorRef> storageNodes;

    SetOfNodesMsg(Map<Integer, ActorRef> storageNodes){
      this.storageNodes = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(storageNodes)); 
    }
  }

  // Request the necessary items for a given nodeId
  public static class GetNecessaryItemsMsg implements Serializable {
    public final int nodeId;

    public GetNecessaryItemsMsg(int nodeId) {
      this.nodeId = nodeId;
    }
  }

  // Response for the necessary items
  public static class NecessaryItemsMsg implements Serializable {
    List<Integer> necessaryItems;

    NecessaryItemsMsg(List<Integer> necessaryItems) {
      this.necessaryItems = Collections.unmodifiableList(new ArrayList<Integer>(necessaryItems));
    }
  }

  // Request for an item during the joining phase
  public static class UpToDateItemRequest implements Serializable {
    public final int key;

    public UpToDateItemRequest(int key) {
      this.key = key;
    }
  }

  // Response for an item during the joining phase
  public static class UpToDateItemResponse implements Serializable {
    public final int key;
    public final Item item;

    public UpToDateItemResponse(int key, String value, int version) {
      this.key = key;
      this.item = new Item(value, version, false);
    }
  }

  // Announces the given nodeId to the whole storage network
  public static class AnnounceJoinMsg implements Serializable {
    public final int nodeId;

    public AnnounceJoinMsg(int nodeId){
      this.nodeId = nodeId;
    }
  }

  public static class LeaveMsg implements Serializable { }

  // Announces that node with id nodeId is leaving. The responsibleFor map
  // contains the items the receiver node becomes responsible for
  public static class AnnounceLeaveMsg implements Serializable {
    public final int nodeId;
    public final Map<Integer,Item> responsibleFor;

    public AnnounceLeaveMsg(int nodeId, Map<Integer, Item> responsibleFor){
      this.nodeId = nodeId;
      this.responsibleFor = Collections.unmodifiableMap(new HashMap<Integer, Item>(responsibleFor)); 
    }
  }


  /*------------------------------------------------------------------------- */
  /*-- Message classes - Replication ---------------------------------------- */
  /*------------------------------------------------------------------------- */

  // Request for an item sent from a storage node to storage node
  public static class ReadRequest implements Serializable {
    public final int key;
    public final int requestId;

    public ReadRequest(int key, int requestId) {
      this.key = key;
      this.requestId = requestId;
    }
  }

  // Response for an item sent from a storage node to storage node that asked it
  public static class ReadResponse implements Serializable {
    public final int key;
    public final Item item;
    public final int requestId;

    public ReadResponse(int key, String value, int version, int requestId) {
      this.key = key;
      this.item = new Item(value, version, false);
      this.requestId = requestId;
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

  public static class ReleaseLockMsg implements Serializable {
    public final ActorRef requester;
    public final int key;

    public ReleaseLockMsg(ActorRef requester, int key) {
      this.requester = requester;   
      this.key = key;
    }
  }  


  /*------------------------------------------------------------------------- */
  /*-- Message classes - Crashing ------------------------------------------- */
  /*------------------------------------------------------------------------- */

  public static class CrashMsg implements Serializable { }

  public static class RecoveryMsg implements Serializable {
    ActorRef bootstrapingRecoveryPeer;

    public RecoveryMsg(ActorRef bootstrapingRecoveryPeer) {
      this.bootstrapingRecoveryPeer = bootstrapingRecoveryPeer;
    }
  }

  /*------------------------------------------------------------------------- */
  /*-- Message handlers - Item repartitioning ------------------------------- */
  /*------------------------------------------------------------------------- */

  private void onJoinMsg(JoinMsg msg) {

    this.nodeId = msg.nodeId;     // save the current node id
    log("Joining the storage network");
    
    // Add myself to the current set of storage nodes
    this.storageNodes.put(this.nodeId, getSelf());

    // The first node joining the storage network has no bootstrapping peer and
    // thus only adds itself to the set of storage nodes
    if (msg.firstNode){
      this.joined = true;
      log("Joined the storage network");
    }else{
      // The joining node contacts the bootstrapping peer to retrieve the current
      // set of nodes constituting the network.
      msg.bootstrappingPeer.tell(new GetSetOfNodesMsg(), getSelf());

    }

  }

  private void onGetSetOfNodesMsg(GetSetOfNodesMsg msg) {
    getSender().tell(new SetOfNodesMsg(storageNodes), getSelf());
  }

  private void onSetOfNodesMsg(SetOfNodesMsg msg) {

    // Save the current set of nodes constituting the network.
    for (int storageNodeId: msg.storageNodes.keySet()) {
      this.storageNodes.put(storageNodeId, msg.storageNodes.get(storageNodeId));
    }

    // If the count of storage nodes in the network is below R, it indicates
    // that the network is in its starting stage, with initial nodes joining. In
    // this phase, nodes don't ask about the data they're responsible for, since
    // it's not possible for a node to find write quorum during this time.
    if (this.storageNodes.size() < R && !this.recoveryMode){
      announceJoin();
    }else {
      // Request data items it is responsible for from its clockwise neighbor
      int clockwiseNeighbor = findClockWiseNeighbor(this.nodeId);
      this.storageNodes.get(clockwiseNeighbor).tell(new GetNecessaryItemsMsg(this.nodeId), getSelf());
    }
    
  }

  private void onGetNecessaryItemsMsg(GetNecessaryItemsMsg msg) {
    List<Integer> necessaryItems = new ArrayList<>(); 

    for (int key : storage.keySet()){
      if (key <= msg.nodeId){
        necessaryItems.add(key);
      }
    }

    // Respond with the list of necessary items
    getSender().tell(new NecessaryItemsMsg(necessaryItems), getSelf());
  }

  private void onNecessaryItemsMsg(NecessaryItemsMsg msg) {
    this.necessaryItems = msg.necessaryItems; // save the set of necessary items

    // Perform read operations to ensure that the items from the clockwise
    // neighbor storage node are up to date    
    for (int key : msg.necessaryItems){
      // contact all the N nodes for the given key
      List<Integer> nodesToBeContacted = findNodesForKey(key);

      for (int storageNodeId : nodesToBeContacted){
        storageNodes.get(storageNodeId).tell(new UpToDateItemRequest(key), getSelf());
      }
    }

    // No message to retrieve from the other nodes
    if (msg.necessaryItems.isEmpty()){
      if (!this.recoveryMode){
        announceJoin();
      }else{
        this.recoveryMode = false;
        log("just recovered");
      }
    }
  }

  private void onUpToDateItemRequest(UpToDateItemRequest msg) {
    int version = 0;
    String value = "";

    // Check if the storage contains the requested item
    if (storage.containsKey(msg.key)) {
      version = storage.get(msg.key).version;
      value = storage.get(msg.key).value;

      // Send the item as a response to the request
      UpToDateItemResponse res = new UpToDateItemResponse(msg.key, value, version);
      getSender().tell(res, getSelf());

    }
  }

  private void onUpToDateItemResponse(UpToDateItemResponse msg) {

    // The node has already joined thus ignore all the join messages that are
    // still flying on the links
    if (this.joined && !this.recoveryMode){
      return;
    }

    // in this first par we use the msg.key as a way to disambiguate between
    // requests
    int requestId = msg.key;

    // if this is the first response create the list to hold the quorum
    if (!quorum.containsKey(requestId)) { 
      quorum.put(requestId, new ArrayList<>());
    }

    Item readResponse = new Item(msg.item.value, msg.item.version, msg.item.lock);
    quorum.get(requestId).add(readResponse);

    // As soon as R replies arrive for all the items that are necessary before
    // joining, the node can finally announce its presence to every node in the
    // system
    boolean receivedAllNecessary = true;
    for (int necessaryItemKey : this.necessaryItems){
      if (quorum.get(necessaryItemKey).size() < R){
        receivedAllNecessary = false;
        break;
      }
    }

    // After receiving all the updated data items, the node can finally announce
    // its presence to every node in the system
    if (receivedAllNecessary){

      // for each necessary item, store in the storage of this new node the most
      // recent version of the item
      for (int necessaryItemKey : this.necessaryItems){
        Item mostRecentItem = quorum.get(necessaryItemKey).get(0);
        int mostRecentVersion = quorum.get(necessaryItemKey).get(0).version;
        
        // find the item with the highest version
        for (Item it : quorum.get(necessaryItemKey)){
          if (it.version > mostRecentVersion){
            mostRecentItem = it;
            mostRecentVersion = it.version;
          }
        }

        // save the most recent item in the storage of the new storage node
        storage.put(necessaryItemKey, mostRecentItem);
      }
      if (!this.recoveryMode){
        announceJoin(); // announce to the whole storage network
      }else{
        this.recoveryMode = false;
        log("just recovered");
      }

    }

  }

  private void onAnnounceJoinMsg(AnnounceJoinMsg msg) {
    
    // Add the new storage node
    storageNodes.put(msg.nodeId, getSender());

    // Eliminate any keys that are no longer required. This involves iterating
    // through all the keys and verifying whether the current node is part of
    // the list of nodes responsible for the given key.
    List<Integer> toRemove = new ArrayList<>();
    for (int key : storage.keySet()){
      List<Integer> nodesForKey = findNodesForKey(key);

      if (!nodesForKey.contains(this.nodeId)){
        toRemove.add(key);
      }
    }
    
    for (int key : toRemove){
      storage.remove(key);
    }

  }
  
  // Handle the command to Leave the network sent from the Main
  private void onLeaveMsg(LeaveMsg msg) {
    
    int leavingNodeId = this.nodeId;

    // Remove this node from the storage net so that storageNodes contains the
    // updated view of the system after the removal of the node. In this way we
    // can use findNodesForKey to find the new nodes that becomes responsible
    // for a key.
    storageNodes.remove(this.nodeId);

    // iterate over all the nodes in the storage network(remember without
    // myself) and send to each of them the AnnounceLeaveMsg message containing
    // the list of items the receiver node becomes responsible for.
    for (int nodeId : storageNodes.keySet()){

      Map<Integer,Item> responsibleFor =  new HashMap<>();

      for (int key : storage.keySet()){
        List<Integer> nodesForKey = findNodesForKey(key);

        // check if nodeId is the node with the highest nodeId in the nodes that
        // are responsible for my(the node leaving) item. In this case it means
        // that the node with the highest id must receive the item because the
        // current node just left
        if (nodesForKey.size()>0 && nodesForKey.get(nodesForKey.size()-1) == nodeId){
          responsibleFor.put(key, new Item(storage.get(key)));
        }

      }

      // announce leave to the node along with the Items it is responsible for
      AnnounceLeaveMsg leaveMsg = new AnnounceLeaveMsg(leavingNodeId, responsibleFor);
      storageNodes.get(nodeId).tell(leaveMsg, getSender());

    }

    log("left the storage network");

  }

  // Handle the announcement of a node leaving the network
  private void onAnnounceLeaveMsg(AnnounceLeaveMsg msg) {
    
    log("Storage node " + msg.nodeId + " left the network"); 

    // if there are some items I should become responsible for then store these
    // items
    if (!msg.responsibleFor.isEmpty()){
      for (int key : msg.responsibleFor.keySet()) {
        log("I became responsible for key: " + key);
        storage.put(key, new Item(msg.responsibleFor.get(key)));  // store the item
      }
    }

    // remove the node from the storage network
    storageNodes.remove(msg.nodeId);

  }


  /*------------------------------------------------------------------------- */
  /*-- Message handlers - Replication --------------------------------------- */
  /*------------------------------------------------------------------------- */
  
  private void onWriteRequest(WriteRequestMsg msg) {
    int version = 0;
    
    // Check if the item is already locked by someone else
    if (storage.containsKey(msg.key) && !storage.get(msg.key).lock){ 
      storage.get(msg.key).lock = true; 
      
      // Save the client that locked the item, also signaling that there is
      // already someone that is working on the key
      lockedBy.put(msg.key, msg.clientNode);  

      version = storage.get(msg.key).version;

      // Send the item as a response to the request
      WriteResponseMsg res = new WriteResponseMsg(version, msg.key, msg.requestId);
      getSender().tell(res, getSelf());
    
    }else if(!lockedBy.containsKey(msg.key)){ 
      // This check is essential to guarantee that when dealing with an item
      // having a key that doesn't exist in the storage (a new key), no other
      // storage node has initiated the process of generating an item with the
      // identical key. If there are no other nodes creating the item, proceed to
      // secure the lock using the 'lockedBy' mechanism.


      // save the client that locked the item, signaling that there is already
      // someone that is working on the item
      lockedBy.put(msg.key, msg.clientNode); 
    
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
  }


  private void onReadRequest(ReadRequest msg) {
    int version = 0;
    String value = "";
      
    // Check if the storage contains the requested item
    if (storage.containsKey(msg.key) && !storage.get(msg.key).lock) { // check if the system is updating the item
      version = storage.get(msg.key).version;
      value = storage.get(msg.key).value;

      // Send the item as a response to the request
      ReadResponse res = new ReadResponse(msg.key, value, version, msg.requestId);
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
    ReadRequest readMsg = new ReadRequest(msg.key, this.requestId);
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
    //save the new item in the storage
    storage.put(msg.key, new Item(msg.item.value, msg.item.version, false));
    
    // unlock the item to state that the creation happened or 
    // the updating from a client finished (setted in onWriteRequest to avoid w-w conflint during creation or ordinary update)
    lockedBy.remove(msg.key);
    log("The item with key " + msg.key + " has been updated to '" + msg.item.value + "' (v" + msg.item.version + ")");
  }

  private void onTimeout(TimeoutMsg msg) {    
    fulfilled.put(msg.requestId, true);
    List<Integer> nodesToBeContacted = findNodesForKey(msg.key);

    // send an error message to the client that originated the request. Check
    // again that the quorum in the meanwhile has not been reached
    if (!quorum.containsKey(msg.requestId) || quorum.get(msg.requestId).size() < msg.minQuorumSize){  
      ErrorMsg error = new ErrorMsg("The request with id " + msg.requestId + " timed out.");
      requestSender.get(msg.requestId).tell(error, getSender());
      
      //send the message to all the node that have been contacted to release the locks enabled during the write request 
      ReleaseLockMsg releaseLockMsg = new ReleaseLockMsg(requestSender.get(msg.requestId), msg.key);
      for (int storageNodeId : nodesToBeContacted){
        storageNodes.get(storageNodeId).tell(releaseLockMsg, getSelf());
      }
    }
  }

  private void onReleaseLock(ReleaseLockMsg msg){
    // permit to unlock the items only to the coordinator that started an update that timed out 
    // the lock request was tracked using the client node reference since an actor can 
    // make a request at the time 
    if (lockedBy.containsKey(msg.key) && lockedBy.get(msg.key) == msg.requester){ 
      lockedBy.remove(msg.key);

      //need to unlock the item lock if the node manage to get the lock on it even if the request timed out 
      if (storage.containsKey(msg.key)){
        storage.get(msg.key).lock = false;
      }
    }
  }

  private void onCrashMsg(CrashMsg msg){
    // change message handler in order to listen only for recovery message
    getContext().become(crashed());
    log("crashed");
  }

  private void onRecoveryMsg(RecoveryMsg msg){
    // reset the message handler to process ordinary message
    getContext().become(createReceive());
    this.recoveryMode = true;
    msg.bootstrapingRecoveryPeer.tell(new GetSetOfNodesMsg(), getSelf());
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

  // Finds the id of the clockwise neighbor of a given node with id nodeId
  int findClockWiseNeighbor(int nodeId){

    int neighborId = -1;

    List<Integer> keySet = new ArrayList<>(storageNodes.keySet());
    Collections.sort(keySet);

    for(int i=0; i<storageNodes.size(); i++){ 
      if (keySet.get(i) >= nodeId){
        neighborId = keySet.get(i);
        break;
      }
    }

    // if there is no bigger node than nodeId(i.e. the new node has the highest
    // id) then the clockwise neighbor is the node with the smallest id(modulo)
    if (neighborId == -1){ 
      neighborId = keySet.get(0);
    }

    return neighborId;
  }

  // Announce the presence of this node to every node in the system
  private void announceJoin(){

    // the node has joined, thus ignore all the future join messages
    this.joined = true; 

    AnnounceJoinMsg announceMsg = new AnnounceJoinMsg(this.nodeId);
    for(int nodeId : storageNodes.keySet()){ 
      if (nodeId == this.nodeId){
        continue;
      }
      storageNodes.get(nodeId).tell(announceMsg, getSelf());
    }
    
    // At the end clear the quorum hashmap to serve future requests
    quorum.clear();

    log("Joined the storage network");
  }


  // log a given message while also printing the storage node id
  void log(String message){
    System.out.println("[S" + this.nodeId + "] " + message);
  }

  // delay the execution for the given amount of milliseconds
  public static void delay(int milliseconds) {
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  // Mapping between the received message types and this actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        /* -- Item repartitioning messages ---------------------------------- */ 
        
        // Join
        .match(JoinMsg.class, this::onJoinMsg)
        .match(GetSetOfNodesMsg.class, this::onGetSetOfNodesMsg)
        .match(SetOfNodesMsg.class, this::onSetOfNodesMsg)
        .match(GetNecessaryItemsMsg.class, this::onGetNecessaryItemsMsg)
        .match(NecessaryItemsMsg.class, this::onNecessaryItemsMsg)
        .match(UpToDateItemRequest.class, this::onUpToDateItemRequest)
        .match(UpToDateItemResponse.class, this::onUpToDateItemResponse)
        .match(AnnounceJoinMsg.class, this::onAnnounceJoinMsg)
        
        // Leave
        .match(LeaveMsg.class, this::onLeaveMsg)
        .match(AnnounceLeaveMsg.class, this::onAnnounceLeaveMsg)

        /* -- Replication messages ------------------------------------------ */ 
        .match(ReadRequest.class, this::onReadRequest)
        .match(UpdateRequestMsg.class, this::onUpdateRequest)
        .match(GetRequestMsg.class, this::onGetRequest)
        .match(ReadResponse.class, this::onReadResponse)
        .match(WriteRequestMsg.class,this::onWriteRequest)
        .match(WriteResponseMsg.class, this::onWriteResponse)
        .match(UpdateResponseMsg.class, this::onUpdateResponse)
        .match(TimeoutMsg.class, this::onTimeout)
        .match(ReleaseLockMsg.class, this::onReleaseLock)
        .match(CrashMsg.class,this::onCrashMsg)
        .build();
  }

  //handler during the crash state of the node
  final AbstractActor.Receive crashed() {
    return receiveBuilder()
        .match(RecoveryMsg.class, this::onRecoveryMsg)
        .matchAny(msg -> {})
        .build();
  } 

}
