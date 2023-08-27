package it.unitn.ds1;

import it.unitn.ds1.ClientNode.GetRequestMsg;
import it.unitn.ds1.ClientNode.UpdateRequestMsg;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageNode extends AbstractActor {

  final static int N = 3;
  final static int R = 2;

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

  private Map<Integer, List<Item>> readQuorum;
  private Map<Integer, List<Item>> writeQuorum;

  /*-- StorageNode constructors --------------------------------------------- */
  public StorageNode(int id) {
    this.id = id;
    this.requestId = 0;

    requestSender = new HashMap<>();
    storageNodes = new HashMap<>();
    storage = new HashMap<>();
    readQuorum = new HashMap<>();
    writeQuorum = new HashMap<>();
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

  public static class ReadRequest implements Serializable { // From storage node to storage node
    public final int key;
    public final int requestId;

    public ReadRequest(int key, int requestId) {
      this.key = key;
      this.requestId = requestId;
    }
  }

  public static class ReadResponse implements Serializable {
    public final int key;
    public final String value;
    public final int version;
    public final int requestId;

    public ReadResponse(int key, String value, int version, int requestId) {
      this.key = key;
      this.value = value;
      this.version = version;
      this.requestId = requestId;
    }
  }

  /*-- Message handlers ----------------------------------------------------- */
  private void onJoinGroupMsg(JoinGroupMsg msg) {
    for (int storageNodeId: msg.storageNodes.keySet()) {
      this.storageNodes.put(storageNodeId, msg.storageNodes.get(storageNodeId));
    }
    System.out.println("[" + id + "] Joining the storage network");
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
    ReadResponse res = new ReadResponse(msg.key, value, version, msg.requestId);
    getSender().tell(res, getSelf());

  }

  private void onReadResponse(ReadResponse msg) {

    int requestId = msg.requestId;

    // if this is the first response create the list to hold the quorum
    if (!readQuorum.containsKey(requestId)) {
      readQuorum.put(requestId, new ArrayList<>());
    }

    Item response = new Item(msg.value, msg.version);
    readQuorum.get(requestId).add(response);

    // As soon as R replies arrive, send the response to the client that
    // originated that request id
    if (readQuorum.get(requestId).size() >= R){
      // TODO: send a GetResponse message and send the msot recent version
      // requestSender.get(requestId).tell(TODO, getSelf());
    }
  }

  private void onGetRequest(GetRequestMsg msg) {

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

    // Contact the N nodes
    List<Integer> nodesToBeContacted = findNodesForKey(msg.key);

    // TODO: implement

  }

  /*-- Auxiliary functions -------------------------------------------------- */

  // Find the N nodes that has to be contacted for a given key
  List<Integer> findNodesForKey(int key) {

    List<Integer> keySet = new ArrayList<>(storageNodes.keySet());
    List<Integer> nodesToBeContacted = new ArrayList<>();
    Collections.sort(keySet);

    for (int i = 0, n = 0; i < storageNodes.size() && n < N; i++) {
      if (keySet.get(i) >= key) {
        nodesToBeContacted.add(keySet.get(i));
        n++;
      }
    }

    // take the remaining items from the beginning of the ring(modulo)
    if (nodesToBeContacted.size() < N) {
      for (int i = 0; i < N - nodesToBeContacted.size(); i++) {
        nodesToBeContacted.add(keySet.get(i));
      }
    }

    return nodesToBeContacted;
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
        .build();
  }

}
