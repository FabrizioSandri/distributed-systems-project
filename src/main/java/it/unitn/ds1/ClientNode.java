package it.unitn.ds1;

import akka.actor.*;
import java.io.Serializable;
import java.util.*;
import it.unitn.ds1.StorageNode.JoinGroupMsg;;

public class ClientNode extends AbstractActor {

  // Client Node variables
  int id;
  private Map<Integer, ActorRef> storageNodes;

  /*-- Actor constructors --------------------------------------------------- */
  public ClientNode(int id) {
    this.id = id;
    storageNodes = new HashMap<>();
    System.out.println("[" + this.id + "] joining the client network");
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

    public UpdateRequestMsg(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class GetRequestMsg implements Serializable {
    // public final int requestId;
    public final int key;
    public GetRequestMsg(int key) {
      this.key = key;
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
      System.out.println(storageNodeId);
      this.storageNodes.put(storageNodeId, msg.storageNodes.get(storageNodeId));
    }
  }

  public void getRequest(Integer storageNodeId, int key) {
    GetRequestMsg m = new GetRequestMsg(key);
    storageNodes.get(storageNodeId).tell(m, getSelf());
  }

  public void updateRequest(Integer storageNodeId, int key, String value) {
    UpdateRequestMsg m = new UpdateRequestMsg(key, value);
    storageNodes.get(storageNodeId).tell(m, getSelf());
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

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onJoinGroupMsg)
        .match(GetResponseMsg.class, this::onGetResponse)
        .match(UpdateResponseMsg.class, this::onUpdateResponse)

        .build();
  }
}
