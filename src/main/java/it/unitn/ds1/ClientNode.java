package it.unitn.ds1;

import akka.actor.*;
import java.io.Serializable;
import java.util.*;
import it.unitn.ds1.StorageNode.JoinGroupMsg;;

public class ClientNode extends AbstractActor {

  // Client Node variables
  private int id;
  private Map<Integer, ActorRef> storageNodes;

  /*-- Actor constructors --------------------------------------------------- */
  public ClientNode(int id) {
    this.id = id;
    storageNodes = new HashMap<>();
    log("Joined the network");
  }

  static public Props props(int id) {
    return Props.create(ClientNode.class, () -> new ClientNode(id));
  }

  /*-- Message classes ------------------------------------------------------ */

  // --requests
  public static class UpdateRequestMsg implements Serializable {
    // public final int requestId;
    public final int key;
    public final String value;
    public final int storageNodeId;

    public UpdateRequestMsg(int key, String value, int storageNodeId) {
      this.key = key;
      this.value = value;
      this.storageNodeId = storageNodeId;
    }
  }

  public static class GetRequestMsg implements Serializable {
    // public final int requestId;
    public final int key;
    public final int storageNodeId;
    public GetRequestMsg(int key, int storageNodeId) {
      this.key = key;
      this.storageNodeId = storageNodeId;
    }
  }

  // --responses
  public static class UpdateResponseMsg implements Serializable {
    // public final int requestId;
    public final int key;
    public final Item item;

    public UpdateResponseMsg(int key, Item item) {
      this.item = item;
      this.key = key;
    }
  }

  public static class GetResponseMsg implements Serializable {
    // public final int requestId;
    public final Item item;

    public GetResponseMsg(Item item) {
      this.item = item;
    }
  }

  /*-- Actor logic ---------------------------------------------------------- */
  private void onJoinGroupMsg(JoinGroupMsg msg) {
    for (int storageNodeId : msg.storageNodes.keySet()) {
      this.storageNodes.put(storageNodeId, msg.storageNodes.get(storageNodeId));
    }
  }

  // forward the get message to the coordinator
  private void onGetRequest(GetRequestMsg msg) {  
    storageNodes.get(msg.storageNodeId).tell(msg, getSelf());
  }

  // forward the update message to the coordinator
  private void onUpdateRequest(UpdateRequestMsg msg) {  
    storageNodes.get(msg.storageNodeId).tell(msg, getSelf());
  }

  private void onGetResponse(GetResponseMsg m) {
    // td keep track of the request-respose id
    System.out
        .println(
            "Get Response for key " + " with value " + m.item.value + " and with version v" + m.item.version);
  }

  private void onUpdateResponse(UpdateResponseMsg m) {
    // td keep track of the request-respose id
    System.out
        .println(
            "Get Response from storage node: " + getSender().path().name() + "for key " + m.key + " with value "
                + m.item.value + " and with version v"
                + m.item.version);
  }

  /*-- Auxiliary functions -------------------------------------------------- */

  // log a given message while also printing the storage node id
  void log(String message){
    System.out.println("[C" + id + "] " + message);
  }

  // Mapping between the received message types and this actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onJoinGroupMsg)
        .match(GetRequestMsg.class, this::onGetRequest)
        .match(GetResponseMsg.class, this::onGetResponse)
        .match(UpdateRequestMsg.class, this::onUpdateRequest)
        .match(UpdateResponseMsg.class, this::onUpdateResponse)

        .build();
  }
}
