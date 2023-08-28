package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.ClientNode.GetRequestMsg;
import it.unitn.ds1.ClientNode.UpdateRequestMsg;
import it.unitn.ds1.StorageNode.JoinGroupMsg;

import java.util.Map;
import java.util.Scanner;
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

    // commands handler main loop
    Scanner console = new Scanner(System.in);
    System.out.println("===============================");
    System.out.println("Syntax for the commands:\n- 'C1 S10 G 5' C1 select storage node 10 as the coordinator for a get request for the key 5\n- 'C1 S10 U 5 hello' C1 select storage node 10 as the coordinator for a update request to the key 5 with the new value 'hello'\n- 'q' to exit ");
    System.out.println("===============================");

    String command = "";
    String[] splitted;

    while(!command.equals("q")){
      System.out.print("> ");
      command = console.nextLine();

      splitted = command.split(" ");

      if (splitted.length == 4 && splitted[2].equals("G")){  // get request   

        int key = Integer.parseInt(splitted[3]);
        int clientnodeId = Integer.parseInt(splitted[0].substring(1));
        int storagenodeId = Integer.parseInt(splitted[1].substring(1));

        GetRequestMsg m = new GetRequestMsg(key, storagenodeId);
        if (clientNodes.containsKey(clientnodeId)){
          clientNodes.get(clientnodeId).tell(m, ActorRef.noSender());
        }else{
          System.out.println("C" + clientnodeId + " doesn't exists in the set of client nodes");
        }

      }else if(splitted.length == 5 && splitted[2].equals("U")){ // update request
        
        int key = Integer.parseInt(splitted[3]);
        String value = splitted[4];
        int clientnodeId = Integer.parseInt(splitted[0].substring(1));
        int storagenodeId = Integer.parseInt(splitted[1].substring(1));

        UpdateRequestMsg m = new UpdateRequestMsg(key, value, storagenodeId);
        if (clientNodes.containsKey(clientnodeId)){
          clientNodes.get(clientnodeId).tell(m, ActorRef.noSender());
        }else{
          System.out.println("C" + clientnodeId + " doesn't exists in the set of client nodes");
        }

      }else if (command.equals("q")){ // quit the application
        System.out.println("Exiting");
      }else {
        System.out.println("Command not recognized as a valid one.");
      }

    }
    
    console.close();

    // system shutdown
    system.terminate();
  }

}
