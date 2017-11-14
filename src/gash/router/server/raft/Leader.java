package gash.router.server.raft;

import java.util.Date;
import java.util.List;
import java.util.Map;

//import common.ConfigurationReader;
//import deven.monitor.client.MonitorClient;
//import deven.monitor.client.MonitorClientApp;
import gash.router.server.NodeMonitor;
import gash.router.server.TopologyStat;
import io.netty.channel.ChannelFuture;
import raft.proto.AppendEntries.AppendEntriesPacket;
import raft.proto.AppendEntries.AppendEntriesResponse.IsUpdated;
//import logger.Logger;
//import raft.proto.AppendEntriesRPC.AppendEntries.RequestType;
//import raft.proto.Monitor.ClusterMonitor;
import raft.proto.Work.WorkMessage;
import routing.Pipe.Route;
import routing.Pipe.Message;
//import server.db.DatabaseService;
//import server.db.Record;
//import server.edges.EdgeInfo;
//import server.queue.ServerQueueService;
import gash.database.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Leader extends Service implements Runnable {

	/********************************************************************************/
	/* Initialisations 															  */
	/********************************************************************************/
	private static Leader INSTANCE = null;
	Thread heartBt = null;
	int heartBeatTime = 1000;
	private int totalResponses = 0;
	private int yesResponses = 0;

	protected static Logger logger = (Logger) LoggerFactory.getLogger("LEADER");
	MongoDB mongoDB;

	/********************************************************************************/
	/* Constructor 																  */
	/********************************************************************************/
	private Leader() {
		// TODO Auto-generated constructor stub
		mongoDB = MongoDB.getInstance();

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
			mongoDB.storeClientMessagetoDB(workMessage);
		}
	}

	// Append Entries to ALL nodes
	private void sendAppendEntriesPacket(WorkMessage wm) {

		System.out.println("Leader****** fn:sendAppendEntriesPacket*****");

		for (Map.Entry<Integer, TopologyStat> entry : NodeMonitor.getInstance().getStatMap().entrySet()) {
			if (entry.getValue().isActive() && entry.getValue().getChannel() != null) {
				ChannelFuture cf = entry.getValue().getChannel().writeAndFlush(wm);
				if (cf.isDone() && !cf.isSuccess()) {
					System.out.println("Failed to send append entries message server " + entry.getValue().getHost());
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
		for (Map.Entry<Integer, TopologyStat> entry : NodeMonitor.getInstance().getStatMap().entrySet()) {
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
		for (Map.Entry<Integer, TopologyStat> entry : NodeMonitor.getInstance().getStatMap().entrySet()) {
			if (entry.getValue().isActive() && entry.getValue().getChannel() != null) {
				WorkMessage workMessage = MessageBuilder.prepareHeartBeat();

				ChannelFuture cf = entry.getValue().getChannel().writeAndFlush(workMessage);
				if (cf.isDone() && !cf.isSuccess()) {
					System.out.println("Failed to send heart beat message to server "+entry.getValue().getHost());
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