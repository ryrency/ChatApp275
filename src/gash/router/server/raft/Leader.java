package gash.router.server.raft;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import gash.router.discovery.DiscoveryServer;
//import common.ConfigurationReader;
//import deven.monitor.client.MonitorClient;
//import deven.monitor.client.MonitorClientApp;
import gash.router.server.NodeMonitor;
import gash.router.server.RemoteNode;
import io.netty.channel.ChannelFuture;
import raft.proto.AppendEntries.AppendEntriesPacket;
import raft.proto.AppendEntries.AppendEntriesResponse.IsUpdated;
//import logger.Logger;
//import raft.proto.AppendEntriesRPC.AppendEntries.RequestType;
//import raft.proto.Monitor.ClusterMonitor;
import raft.proto.Work.WorkMessage;
import routing.Pipe.Route;
import routing.Pipe.User;
import routing.Pipe.Message;
//import server.db.DatabaseService;
//import server.db.Record;
//import server.edges.EdgeInfo;
//import server.queue.ServerQueueService;
import gash.database.*;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class Leader extends Service implements Runnable {
	
	/********************************************************************************/
	/* Initialisations 															  */
	/********************************************************************************/
	private static Leader INSTANCE = null;
	Thread heartBt = null;
	int heartBeatTime = 1000;
	private int totalResponses = 0;
	private int yesResponses = 0;
	
	public static final int REGISTER = 0;
	public static final int ACCESS = 1;
	public static final int DELETE = 2;




	protected static Logger logger = (Logger) LoggerFactory.getLogger("LEADER");
	MessageMongoDB messageMongoDB;
	UserMongoDB userMongoDB;

	/********************************************************************************/
	/* Constructor 																  */
	/********************************************************************************/
	private Leader() {
		// TODO Auto-generated constructor stub
		messageMongoDB = MessageMongoDB.getInstance();
		userMongoDB = UserMongoDB.getInstance();
		DiscoveryServer udpDiscoveryServer = new DiscoveryServer(
				RaftNode.getInstance().getState().getConf(), 
				RaftNode.getInstance().getState().getNodeConf()
		);
		
		Thread discoveryThread = new Thread(udpDiscoveryServer);
		discoveryThread.start();

	}

	/********************************************************************************/
	/* Get Instance of Leader to ensure single instance!! 						  */
	/********************************************************************************/
	public static Leader getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new Leader();
		}
		return INSTANCE;
	}

	/********************************************************************************/
	/* Starting Leader Thread!! 													  */
	/********************************************************************************/
	@Override
	public void run() {
		logger.info("***Leader Started***");
		System.out.println("Leader: Current term " + NodeState.currentTerm);

		// NodeState.currentTerm++;
		// initLatestTimeStampOnUpdate();
		heartBt = new Thread() {
			public void run() {
				while (running) {
					sendHeartBeat();
					try {

						Thread.sleep(heartBeatTime);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};

		heartBt.start();
		// Create connection to database
	}

	/********************************************************************************/
	/* Handling Users - Registration, Access & Deletion                             */
	/********************************************************************************/
	
	public void handleUsers(Route msg) {
		User.Builder userPacket = User.newBuilder();

		if (userPacket.getAction().getNumber() == REGISTER) {
			RegisterUser(msg); /* New User .. so check for duplicate and if not, add entry into DB */
		}
		if (userPacket.getAction().getNumber() == ACCESS) {
			// AccessUser(msg); /*RENCY - NEED TO CHECK */
		}
		if (userPacket.getAction().getNumber() == DELETE) {
			DeleteUser(msg);
		}
	}

	/*----------------------------------------------------*/
	/*Register New User into DB if does not exist already */
	/*----------------------------------------------------*/
	public void RegisterUser(Route msg) {

		FindIterable<Document> result = null;
		result = userMongoDB.get(msg.getUser().getUname());
		if (result == null) {
			userMongoDB.storeUserMessagetoDB(msg);
		} else {
			String error = "User Already Registered";
			/* Rency - SEND ERROR MESSAGE TO USER */
			/* Rency - NEED TO HANDLE USER REPLICATION */
			MessageBuilder.prepareMessage(error); /* RENCY - NOT COMPLETE */
		}

	}

	
	/*----------------------------------------------------*/
	/* send all unread messages for the user */
	/*----------------------------------------------------*/

	public void handleMessageRequest(Route msg) {
		String userName = msg.getMessagesRequest().getId();

		FindIterable<Document> dbresults = null;

		dbresults = userMongoDB.get(userName);
		if (dbresults != null) {
			dbresults = messageMongoDB.getUnreadMessage(userName);
		}

		MessageBuilder.prepareMessageResponse(dbresults);
		/* CODE TO SEND DATA TO CLIENT */

	}

	/*----------------------------------------------------*/
	/* Delete user Registration */
	/*----------------------------------------------------*/
	public void DeleteUser(Route msg) {
		userMongoDB.delete(msg.getUser().getUname());
	}
	
	/********************************************************************************/
	/* Handling(replicating and storing) message sent by sender client              */
	/* and send Append Entries to all Followers                                     */
	/********************************************************************************/
	public void handleClientRequest(Route clientRoute) {
		System.out.println("***Leader*** fn:handleClientMessage");
		if (clientRoute.hasMessage()) {
			System.out.println("***Leader*** fn:handleClientMessage *** Inside If client hasMessage");
			WorkMessage workMessage = MessageBuilder.prepareAppendEntriesPacket(clientRoute,
					clientRoute.getMessage().getTimestamp());
			System.out.println("***Leader*** fn:handleClientMessage *** work message returned");
			sendAppendEntriesPacket(workMessage);
			messageMongoDB.storeClientMessagetoDB(workMessage);
		}
	}

	// Append Entries to ALL nodes
	private void sendAppendEntriesPacket(WorkMessage wm) {

		System.out.println("Leader****** fn:sendAppendEntriesPacket*****");

		for (Map.Entry<Integer, RemoteNode> entry : NodeMonitor.getInstance().getNodeMap().entrySet()) {
			if (entry.getValue().isActive() && entry.getValue().getChannel() != null) {
				ChannelFuture cf = entry.getValue().getChannel().writeAndFlush(wm);
				if (cf.isDone() && !cf.isSuccess()) {
					System.out.println("Failed to send append entries message server " + entry.getValue().getNodeConf().getHost());
				}
			}
		}
	}
	
	/********************************************************************************/
	/* Handling Append Entries Response Packets                                     */
	/********************************************************************************/
	
	@Override
	public void handleAppendEntries(WorkMessage wm) {
		totalResponses++;
		if (wm.getAppendEntriesPacket().getAppendEntriesResponse().getIsUpdated() == IsUpdated.YES) {
			yesResponses++;
		}
		/*NEED TO COMPLETE THIS CODE*NO MESSAGE ID?????? */			
	}
	public int countActiveNodes() {
		int count = 0;
		for (Map.Entry<Integer, RemoteNode> entry : NodeMonitor.getInstance().getNodeMap().entrySet()) {
			if (entry.getValue().isActive() && entry.getValue().getChannel() != null) {	
				count++;
				
			}
		}
		return count;
	}

	/********************************************************************************/
	/* Handling Heartbeat                                                           */
	/********************************************************************************/
	@Override
	public void sendHeartBeat() {
		/*Sending HeartBeat to all Followers to inform them of the health of Leader */
		for (Map.Entry<Integer, RemoteNode> entry : NodeMonitor.getInstance().getNodeMap().entrySet()) {
			if (entry.getValue().isActive() && entry.getValue().getChannel() != null) {
				WorkMessage workMessage = MessageBuilder.prepareHeartBeat();

				ChannelFuture cf = entry.getValue().getChannel().writeAndFlush(workMessage);
				if (cf.isDone() && !cf.isSuccess()) {
					System.out.println("Failed to send heart beat message to server "+entry.getValue().getNodeConf().getHost());
				}
			}
		}
	}


	public void handleHeartBeat(WorkMessage wm) {
		/* If Leader receives a HB, it indicates that the node is no longer a leader */

		if (wm.getHeartBeatPacket().getHeartbeat().getTerm() >= NodeState.currentTerm) {
			NodeState.getInstance().setState(NodeState.FOLLOWER);
		}
	}


	/********************************************************************************/
	/* Starting Leader Service 													  */
	/********************************************************************************/

	public void startService(Service service) {
		running = Boolean.TRUE;
		cthread = new Thread((Leader) service);
		cthread.start();
	}

	/********************************************************************************/
	/* Stoping Leader Service                                       				  */
	/********************************************************************************/
	public void stopService() {
		running = Boolean.FALSE;

	}

}