package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.StorageNode.JoinGroupMsg;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

public class Main {

  final static int N_STORAGE_NODES = 5;
  final static int N_CLIENT_NODES = 5;

  public static void main(String[] args) {

    // Create the actor system
    final ActorSystem system = ActorSystem.create("system");

    // Create the client nodes
    Map<Integer, ActorRef> clientNodes = new HashMap<Integer, ActorRef>();
    clientNodes.put(1, system.actorOf(ClientNode.props(1), "c1"));
    clientNodes.put(2, system.actorOf(ClientNode.props(2), "c2"));

    // Create the storage nodes
    Map<Integer, ActorRef> storageNodes = new HashMap<Integer, ActorRef>();
    storageNodes.put(10, system.actorOf(StorageNode.props(10), "s10"));
    storageNodes.put(20, system.actorOf(StorageNode.props(20), "s20"));
    storageNodes.put(30, system.actorOf(StorageNode.props(30), "s30"));
    storageNodes.put(40, system.actorOf(StorageNode.props(40), "s40"));

    // Send join messages to all the storage nodes to inform them about the
    // whole storage network nodes
    JoinGroupMsg start = new JoinGroupMsg(storageNodes);
    for (int storageNodeId : storageNodes.keySet()) {
      storageNodes.get(storageNodeId).tell(start, ActorRef.noSender());
    }

    for (int clientNodesId : clientNodes.keySet()) {
      clientNodes.get(clientNodesId).tell(start, ActorRef.noSender());
    }

    // system shutdown
    system.terminate();
  }

}
