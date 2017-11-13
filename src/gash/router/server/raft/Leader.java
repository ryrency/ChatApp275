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
//import logger.Logger;
//import raft.proto.AppendEntriesRPC.AppendEntries.RequestType;
//import raft.proto.Monitor.ClusterMonitor;
import raft.proto.Work.WorkMessage;
import routing.Payload;
import routing.Payload.ClientRoute;
import routing.Payload.Message;
//import server.db.DatabaseService;
//import server.db.Record;
//import server.edges.EdgeInfo;
//import server.queue.ServerQueueService;
import gash.database.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Leader extends Service implements Runnable {

	private static Leader INSTANCE = null;
	Thread heartBt = null;
	int heartBeatTime = 1000;
	protected static Logger logger = (Logger) LoggerFactory.getLogger("LEADER");
	MongoDB mongoDB;


	private Leader() {
		// TODO Auto-generated constructor stub
		mongoDB = MongoDB.getInstance();

	}

	public static Leader getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new Leader();
		}
		return INSTANCE;
	}

	@Override
	public void run() {
		logger.info("***Leader Started***");
		System.out.println("Leader: Current term "+NodeState.currentTerm);

//		NodeState.currentTerm++;
		//initLatestTimeStampOnUpdate();
		heartBt = new Thread(){
		    public void run(){
				while (running) {
					try {
						
						Thread.sleep(heartBeatTime);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					sendHeartBeat();
				}
		    }
		 };

		heartBt.start();
		//Create connection to database
		//ServerQueueService.getInstance().createQueue();
	}

	//Append Entries to ALL nodes 
	private void sendAppendEntriesPacket(WorkMessage wm) {
		
		System.out.println("Leader****** fn:sendAppendEntriesPacket*****");

        for (Map.Entry<Integer, TopologyStat> entry :NodeMonitor.getInstance().getStatMap().entrySet()) {
            if (entry.getValue().isActive() && entry.getValue().getChannel() != null) {                    
                ChannelFuture cf = entry.getValue().getChannel().writeAndFlush(wm);
                if (cf.isDone() && !cf.isSuccess()) {
                    System.out.println("Failed to send append entries message server "+entry.getValue().getHost());
                }
            }
        }
}

//	public void handleHeartBeatResponse(WorkMessage wm) {
//
//		long timeStampOnLatestUpdate = wm.getHeartBeatPacket().getHeartBeatResponse().getTimeStampOnLatestUpdate();
//
//		if (DatabaseService.getInstance().getDb().getCurrentTimeStamp() > timeStampOnLatestUpdate) {
//			List<Record> laterEntries = DatabaseService.getInstance().getDb().getNewEntries(timeStampOnLatestUpdate);
//
//			for (EdgeInfo ei : NodeState.getInstance().getServerState().getEmon().getOutboundEdges().getMap()
//					.values()) {
//
//				if (ei.isActive() && ei.getChannel() != null
//						&& ei.getRef() == wm.getHeartBeatPacket().getHeartBeatResponse().getNodeId()) {
//
//					for (Record record : laterEntries) {
//						WorkMessage workMessage = ServiceUtils.prepareAppendEntriesPacket(record.getKey(),
//								record.getImage(), record.getTimestamp(), RequestType.POST);
//						Logger.DEBUG("Sent AppendEntriesPacket to " + ei.getRef() + "for the key (later Entries) "
//								+ record.getKey());
//						ChannelFuture cf = ei.getChannel().writeAndFlush(workMessage);
//						if (cf.isDone() && !cf.isSuccess()) {
//							Logger.DEBUG("failed to send message (AppendEntriesPacket) to server");
//						}
//					}
//				}
//			}
//
//		}
//
//	}
//	
//	public void handleHeartBeat(WorkMessage wm) {
//		Logger.DEBUG("HeartbeatPacket received from leader :" + wm.getHeartBeatPacket().getHeartbeat().getLeaderId());
//		//onReceivingHeartBeatPacket();
//		WorkMessage heartBeatResponse = ServiceUtils.prepareHeartBeatResponse();
//		
//		for (EdgeInfo ei : NodeState.getInstance().getServerState().getEmon().getOutboundEdges().getMap().values()) {
//
//			if (ei.isActive() && ei.getChannel() != null
//					&& ei.getRef() == wm.getHeartBeatPacket().getHeartbeat().getLeaderId()) {
//					if(wm.getHeartBeatPacket().getHeartbeat().getTerm()>=NodeState.currentTerm) {
//						NodeState.getInstance().setState(NodeState.FOLLOWER);
//					}
////				Logger.DEBUG("Sent HeartBeatResponse to " + ei.getRef());
////				ChannelFuture cf = ei.getChannel().writeAndFlush(heartBeatResponse);
////				if (cf.isDone() && !cf.isSuccess()) {
////					Logger.DEBUG("failed to send message (HeartBeatResponse) to server");
////				}
//			}
//		}
//
//	}
//
	@Override
	public void sendHeartBeat() {
		
		System.out.println("Leader:  term -> "+NodeState.currentTerm);
		for (Map.Entry<Integer, TopologyStat> entry :NodeMonitor.getInstance().getStatMap().entrySet()) {
			if (entry.getValue().isActive() && entry.getValue().getChannel() != null) {
				WorkMessage workMessage = MessageBuilder.prepareHeartBeat();
				
				ChannelFuture cf = entry.getValue().getChannel().writeAndFlush(workMessage);
				if (cf.isDone() && !cf.isSuccess()) {
					System.out.println("Fail to send heart beat message to other server");
				}
			}
		}
		
		//Removing monitor??
//		if (ConfigurationReader.getInstance().getMonitorHost() != null && ConfigurationReader.getInstance().getMonitorPort() != null) {
//			sendClusterMonitor(ConfigurationReader.getInstance().getMonitorHost(), ConfigurationReader.getInstance().getMonitorPort());
//		}		
	}
//	
////	public void sendClusterMonitor(String host, int port) {
////		try {
////			MonitorClient mc = new MonitorClient(host, port);
////			MonitorClientApp ma = new MonitorClientApp(mc);
////			// do stuff w/ the connection
////			System.out.println("Creating message");
////			ClusterMonitor msg = ma.sendDummyMessage(countActiveNodes(),NodeState.getupdatedTaskCount());
////			System.out.println("Sending generated message");
////			mc.write(msg);	
////		}catch(Exception e) {
////			e.printStackTrace();
////		}
////		
////		
////	}
	public int countActiveNodes() {
		int count = 0;
		for (Map.Entry<Integer, TopologyStat> entry :NodeMonitor.getInstance().getStatMap().entrySet()) {

			if (entry.getValue().isActive() && entry.getValue().getChannel() != null) {				
				count++;
			}
		}
		return count;
	}

//	public byte[] handleGetMessage(String key) {
//		System.out.println("GET Request Processed by Node: " + NodeState.getInstance().getServerState().getConf().getNodeId());
//		NodeState.updateTaskCount();
//		return DatabaseService.getInstance().getDb().get(key);
//	}
//	
//	public String handlePostMessage(byte[] image, long timestamp) {
//		System.out.println("POST Request Processed by Node: " + NodeState.getInstance().getServerState().getConf().getNodeId());
//		NodeState.updateTaskCount();
//		NodeState.setTimeStampOnLatestUpdate(timestamp);
//		String key = DatabaseService.getInstance().getDb().post(image, timestamp);
//		WorkMessage wm = ServiceUtils.prepareAppendEntriesPacket(key, image, timestamp, RequestType.POST);
//		sendAppendEntriesPacket(wm);
//		return key;
//	}
//
//	public void handlePutMessage(String key, byte[] image, long timestamp) {
//		System.out.println("PUT Request Processed by Node: " + NodeState.getInstance().getServerState().getConf().getNodeId());
//		NodeState.updateTaskCount();
//		NodeState.setTimeStampOnLatestUpdate(timestamp);
//		DatabaseService.getInstance().getDb().put(key, image, timestamp);
//		WorkMessage wm = ServiceUtils.prepareAppendEntriesPacket(key, image, timestamp, RequestType.PUT);
//		sendAppendEntriesPacket(wm);
//	}
//	
//	@Override
//	public void handleDelete(String key) {
//		System.out.println("DELETE Request Processed by Node: " + NodeState.getInstance().getServerState().getConf().getNodeId());
//		NodeState.updateTaskCount();
//		NodeState.setTimeStampOnLatestUpdate(System.currentTimeMillis());
//		DatabaseService.getInstance().getDb().delete(key);
//		WorkMessage wm = ServiceUtils.prepareAppendEntriesPacket(key, null, 0 ,RequestType.DELETE);
//		sendAppendEntriesPacket(wm);
//	}
	
	// What should be the key?? Currently using receiver id
	
	//
	public void handleClientRequest(ClientRoute clientRoute) {
		System.out.println("***Leader*** fn:handleClientMessage");
		if(clientRoute.hasMessage()){
			System.out.println("***Leader*** fn:handleClientMessage *** Inside If client hasMessage");
			WorkMessage workMessage = MessageBuilder.prepareAppendEntriesPacket(clientRoute, clientRoute.getMessage().getTimestamp());
			System.out.println("***Leader*** fn:handleClientMessage *** work message returned");
			sendAppendEntriesPacket(workMessage);
			mongoDB.storeClientMessagetoDB(workMessage);
		}	
	}
	
	public void handleHeartBeat(WorkMessage wm) {
        if(wm.getHeartBeatPacket().getHeartbeat().getTerm()>=NodeState.currentTerm) {
            NodeState.getInstance().setState(NodeState.FOLLOWER);
            }
    }
	
	
	
	public void handleFollowerMessage() {
		
	}

	public void startService(Service service) {
		running = Boolean.TRUE;
		cthread = new Thread((Leader) service);
		cthread.start();
	}

	public void stopService() {
		running = Boolean.FALSE;

	}

}