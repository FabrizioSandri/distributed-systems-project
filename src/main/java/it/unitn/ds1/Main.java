package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.ClientNode.GetRequestMsg;
import it.unitn.ds1.ClientNode.UpdateRequestMsg;
import it.unitn.ds1.StorageNode.JoinMsg;
import it.unitn.ds1.ClientNode.JoinGroupMsg;

import java.util.Map;
import java.util.Scanner;
import java.util.HashMap;

public class Main {

  public static void main(String[] args) {

    // Create the actor system
    final ActorSystem system = ActorSystem.create("system");

    // Create the client nodes
    Map<Integer, ActorRef> clientNodes = new HashMap<Integer, ActorRef>();
    clientNodes.put(1, system.actorOf(ClientNode.props(1), "c1"));
    clientNodes.put(2, system.actorOf(ClientNode.props(2), "c2"));

    JoinGroupMsg clientJoin = new JoinGroupMsg();
    for (int clientNodesId : clientNodes.keySet()) {
      clientNodes.get(clientNodesId).tell(clientJoin, ActorRef.noSender());
    }

    // Create the storage nodes
    Map<Integer, ActorRef> storageNodes = new HashMap<Integer, ActorRef>();


    // commands handler main loop
    Scanner console = new Scanner(System.in);
    System.out.println("===============================");
    System.out.println("Syntax for the commands:\n- 'J 5 10' add a new storage node with id 5 to the storage network using node 10 as bootstrapping peer. If the node is the first one, leave the bootstrapping peer parameter empty\n- 'C1 S10 G 5' C1 select storage node 10 as the coordinator for a get request for the key 5\n- 'C1 S10 U 5 hello' C1 select storage node 10 as the coordinator for a update request to the key 5 with the new value 'hello'\n- 'q' to exit ");
    System.out.println("===============================");

    String command = "";

    while(!command.equals("q")){
      System.out.print("> ");
      command = console.nextLine();
      String[] splitted = command.split(" ");

      if (splitted[0].equals("J")) {  // join of a new storage node

        int newNodeId = Integer.parseInt(splitted[1]);
        
        if (storageNodes.containsKey(newNodeId)){
          log("A storage node with the same id already exists.");
          continue;
        }
        storageNodes.put(newNodeId, system.actorOf(StorageNode.props(), "s"+newNodeId));

        JoinMsg storageJoin;
        if (splitted.length == 2){  // first node to join
          storageJoin = new JoinMsg(newNodeId, null, true);
          storageNodes.get(newNodeId).tell(storageJoin, ActorRef.noSender());
        }else if(splitted.length == 3) {
          int bootstrappingPeerId = Integer.parseInt(splitted[2]);
          if (storageNodes.containsKey(bootstrappingPeerId)){
            storageJoin = new JoinMsg(newNodeId, storageNodes.get(bootstrappingPeerId), false);
            storageNodes.get(newNodeId).tell(storageJoin, ActorRef.noSender());
          }else {
            log("The specified storage node doesn't exists.");
          }
        }


      }else if (splitted.length == 4 && splitted[2].equals("G")){  // get request   

        int key = Integer.parseInt(splitted[3]);
        int clientnodeId = Integer.parseInt(splitted[0].substring(1));
        int storagenodeId = Integer.parseInt(splitted[1].substring(1));

        GetRequestMsg m = new GetRequestMsg(key, storageNodes.get(storagenodeId));
        if (clientNodes.containsKey(clientnodeId)){
          clientNodes.get(clientnodeId).tell(m, ActorRef.noSender());
        }else{
          log("C" + clientnodeId + " doesn't exists in the set of client nodes");
        }

      }else if(splitted.length == 5 && splitted[2].equals("U")){ // update request
        
        int key = Integer.parseInt(splitted[3]);
        String value = splitted[4];
        int clientnodeId = Integer.parseInt(splitted[0].substring(1));
        int storagenodeId = Integer.parseInt(splitted[1].substring(1));

        UpdateRequestMsg m = new UpdateRequestMsg(key, value, storageNodes.get(storagenodeId));
        if (clientNodes.containsKey(clientnodeId)){
          clientNodes.get(clientnodeId).tell(m, ActorRef.noSender());
        }else{
          log("C" + clientnodeId + " doesn't exists in the set of client nodes");
        }

      }else if (command.equals("q")){ // quit the application
        log("Exiting");
      }else {
        log("Command not recognized as a valid one.");
      }

      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }
    
    console.close();

    // system shutdown
    system.terminate();
  }

    // log a given message while also printing MAIN
    static void log(String message){
      System.out.println("[MAIN] " + message);
    }

}
