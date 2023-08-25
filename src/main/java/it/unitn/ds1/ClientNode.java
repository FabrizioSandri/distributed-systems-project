package it.unitn.ds1;

import akka.actor.*;
import java.io.Serializable;
import java.util.*;

public class ClientNode extends AbstractActor {

  // Client Node variables
  int id;

  /*-- Actor constructors --------------------------------------------------- */
  public ClientNode(int id) {
    this.id = id;
    System.out.println("Client " + id + " joined with name: " + (this.getSelf()).path().name());

  }

  static public Props props(int id) {
    return Props.create(ClientNode.class, () -> new ClientNode(id));
  }

  /*-- Message classes ------------------------------------------------------ */

  public static class GetItemMessage implements Serializable {
    public final int key;

    public GetItemMessage(int key) {
      this.key = key;
    }
  }

  public static class UpdateItemMessage implements Serializable {
    // public final int requestID
    public final int key;
    public final String value;

    public UpdateItemMessage(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  /*-- Actor logic ---------------------------------------------------------- */

  @Override
  public void preStart() {
  }

  // TODO: create message in function instead of get it as parameter
  public void getRequest(Serializable m, ActorRef receiver) {
    receiver.tell(m, getSelf());
  }
  // TODO: create message in function instead of get it as parameter
  public void updateRequest(Serializable m, ActorRef receiver) {
    receiver.tell(m, getSelf());
  }
  // Here we define the mapping between the received message types
  // and our actor methods

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .build();
  }
}
