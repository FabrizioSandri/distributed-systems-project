package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.StorageNode.JoinGroupMsg;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class Main {

  final static int N_STORAGE_NODES = 5;
  final static int N_CLIENT_NODES = 5;

  public static void main(String[] args) {

    // Create the actor system
    final ActorSystem system = ActorSystem.create("system");

    // Create the client nodes
    List<ActorRef> clientNodes = new ArrayList<>();
    clientNodes.add(system.actorOf(ClientNode.props(1), "c1"));
    clientNodes.add(system.actorOf(ClientNode.props(2), "c2"));

    // Create the storage nodes
    List<ActorRef> storageNodes = new ArrayList<>();
    storageNodes.add(system.actorOf(StorageNode.props(10), "s10"));
    storageNodes.add(system.actorOf(StorageNode.props(20), "s20"));
    storageNodes.add(system.actorOf(StorageNode.props(30), "s30"));
    storageNodes.add(system.actorOf(StorageNode.props(40), "s40"));

    // Send join messages to all the storage nodes to inform them about the
    // whole storage network nodes
    JoinGroupMsg start = new JoinGroupMsg(storageNodes);
    for (ActorRef s : storageNodes) {
      s.tell(start, ActorRef.noSender());
    }

    // system shutdown
    system.terminate();
  }

}
