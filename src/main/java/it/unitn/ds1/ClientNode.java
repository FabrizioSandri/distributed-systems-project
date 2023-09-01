package it.unitn.ds1;

import akka.actor.*;
import java.io.Serializable;

public class ClientNode extends AbstractActor {

  // Client Node variables
  private int id;

  /*-- Actor constructors --------------------------------------------------- */
  public ClientNode(int id) {
    this.id = id;
  }

  static public Props props(int id) {
    return Props.create(ClientNode.class, () -> new ClientNode(id));
  }

  /*-- Message classes ------------------------------------------------------ */
  public static class JoinGroupMsg implements Serializable { }

  // --requests
  public static class UpdateRequestMsg implements Serializable {
    // public final int requestId;
    public final int key;
    public final String value;
    public final ActorRef storageNodeRef;

    public UpdateRequestMsg(int key, String value, ActorRef storageNodeRef) {
      this.key = key;
      this.value = value;
      this.storageNodeRef = storageNodeRef;
    }
  }

  public static class GetRequestMsg implements Serializable {
    public final int key;
    public final ActorRef storageNodeRef;

    public GetRequestMsg(int key, ActorRef storageNodeRef) {
      this.key = key;
      this.storageNodeRef = storageNodeRef;
    }
  }

  // --responses
  public static class UpdateResponseMsg implements Serializable {
    public final int key;
    public final Item item;

    public UpdateResponseMsg(int key, Item item) {
      this.item = item;
      this.key = key;
    }
  }

  public static class GetResponseMsg implements Serializable {
    public final int key;
    public final Item item;

    public GetResponseMsg(int key, Item item) {
      this.key = key;
      this.item = item;
    }
  }

  public static class ErrorMsg implements Serializable {
    public final String errormsg;

    public ErrorMsg(String errormsg) {
      this.errormsg = errormsg;
    }
  }

  /*-- Actor logic ---------------------------------------------------------- */
  private void onJoinGroupMsg(JoinGroupMsg msg) {
    log("Joined the network");
  }

  // forward the get message to the coordinator
  private void onGetRequest(GetRequestMsg msg) {  
    msg.storageNodeRef.tell(msg, getSelf());
  }

  // forward the update message to the coordinator
  private void onUpdateRequest(UpdateRequestMsg msg) {  
    msg.storageNodeRef.tell(msg, getSelf());
  }

  private void onGetResponse(GetResponseMsg msg) {
    log("get(" + msg.key + "): " + msg.item.value + " (v" + msg.item.version + ")");
  }

  private void onUpdateResponse(UpdateResponseMsg msg) {
    log("update(" + msg.key + ", " + msg.item.value + "): " + msg.item.value + " (v" + msg.item.version + ")");
  }

  // Handle error messages
  private void onErrorMsg(ErrorMsg msg) {  
    log(msg.errormsg);
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
        .match(ErrorMsg.class, this::onErrorMsg)
        .build();
  }
}
