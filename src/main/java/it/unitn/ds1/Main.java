package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.ClientNode.GetRequestMsg;
import it.unitn.ds1.ClientNode.UpdateRequestMsg;
import it.unitn.ds1.StorageNode.JoinMsg;
import it.unitn.ds1.StorageNode.LeaveMsg;
import it.unitn.ds1.ClientNode.JoinGroupMsg;
import it.unitn.ds1.StorageNode.CrashMsg;
import it.unitn.ds1.StorageNode.RecoveryMsg;

import java.util.Map;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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


    // First parse the file commands.txt containing the list of commands to
    // execute
    try (BufferedReader br = new BufferedReader(new FileReader("commands.txt"))) {
      String command;
      while ((command = br.readLine()) != null) {
        parseCommand(command, system, storageNodes, clientNodes); // For example, you can print the line
      }
    } catch (IOException e) {
      log("The 'commands.txt' doesn't exists. Proceeding to execute the command line version of the program.");
    }


    // commands handler main loop
    Scanner console = new Scanner(System.in);
    System.out.println("===============================");
    System.out.println("Syntax for the commands:\n- 'J 5 10' add a new storage node with id 5 to the storage network using node 10 as bootstrapping peer. If the node is the first one, leave the bootstrapping peer parameter empty\n- 'L 5' tell node 5 to leave the storage network \n- 'C1 10 G 5' C1 select storage node 10 as the coordinator for a get request for the key 5\n- 'C1 10 U 5 hello' C1 select storage node 10 as the coordinator for a update request to the key 5 with the new value 'hello'\n- 'C 2' make the storage node with id eqauls 2 crash\n- 'R 2 3' Make the storage node with id equals 2 recover using the storage node with id equals 3 as bootstrapping recovery peer\n- 'q' to exit ");
    System.out.println("===============================");

    String command = "";

    while(!command.equals("q")){
      System.out.print("> ");
      command = console.nextLine();

      parseCommand(command, system, storageNodes, clientNodes);
    }
    
    console.close();

    // system shutdown
    system.terminate();
  }

  
  public static void parseCommand(String command, final ActorSystem system, Map<Integer, ActorRef> storageNodes, Map<Integer, ActorRef> clientNodes) {
      String[] splitted = command.split(" ");

      if (splitted[0].equals("J")) {  // join of a new storage node
        
        if (splitted.length == 2 && storageNodes.size() > 1){
          log("You must supply the bootstrapping peer.");
          return;
        }

        int newNodeId = Integer.parseInt(splitted[1]);
        
        if (storageNodes.containsKey(newNodeId)){
          log("A storage node with the same id already exists.");
          return;
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
            log("The bootstrapping peer that you sepcified doesn't exists.");
          }
        }

      }else  if (splitted[0].equals("L")) {  // Leave operation

        int leavingNodeId = Integer.parseInt(splitted[1]);
        
        if (!storageNodes.containsKey(leavingNodeId)){
          log("Storage node with id " + leavingNodeId + " doesn't exists.");
          return;
        }
        
        LeaveMsg leaveMsg = new LeaveMsg();
        storageNodes.get(leavingNodeId).tell(leaveMsg, ActorRef.noSender());
        storageNodes.remove(leavingNodeId);

      }else if (splitted.length == 4 && splitted[2].equals("G")){  // get request   

        int key = Integer.parseInt(splitted[3]);
        int clientnodeId = Integer.parseInt(splitted[0].substring(1));
        int storagenodeId = Integer.parseInt(splitted[1]);

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
        int storagenodeId = Integer.parseInt(splitted[1]);

        UpdateRequestMsg m = new UpdateRequestMsg(key, value, storageNodes.get(storagenodeId));
        if (clientNodes.containsKey(clientnodeId)){
          clientNodes.get(clientnodeId).tell(m, ActorRef.noSender());
        }else{
          log("C" + clientnodeId + " doesn't exists in the set of client nodes");
        }

      }else if(command.equals("C")){
        int nodeId = Integer.parseInt(splitted[1]);
        if (!storageNodes.containsKey(nodeId)){
          log("The storage node with that id does not exists.");
          return;
        }

        storageNodes.get(nodeId).tell(new CrashMsg(), ActorRef.noSender()); 
      }else if(command.equals("R")){

        int nodeId = Integer.parseInt(splitted[1]);
        int bootstrappingRecoveryPeerId = Integer.parseInt(splitted[2]);
        if (!storageNodes.containsKey(nodeId) || !storageNodes.containsKey(bootstrappingRecoveryPeerId)){
          log("One or both specified node id don't exists.");
          return;
        }

        storageNodes.get(nodeId).tell(new RecoveryMsg(storageNodes.get(bootstrappingRecoveryPeerId)), ActorRef.noSender()); 
      }else  if (splitted[0].equals("D")) {
        StorageNode.delay(Integer.parseInt(splitted[1]));
      }else if (command.equals("q")){ // quit the application
        log("Exiting");
      }else if(command.equals("")){
        return;
      }else {
        log("Command not recognized as a valid one.");
      }


  }

  // log a given message while also printing MAIN
  static void log(String message){
    System.out.println("[MAIN] " + message);
  }

}
