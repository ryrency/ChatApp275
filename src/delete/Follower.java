package delete;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.proto.Vote.ResponseVote;
import raft.proto.Work.WorkMessage;
import io.netty.channel.ChannelFuture;
import message.database.MessageMongoDB;
import message.server.NodeMonitor;
import message.server.RemoteNode;
import message.server.raft.MessageBuilder;

public class Follower extends Service implements Runnable {

	/********************************************************************************/
	/* Initialisations */
	/********************************************************************************/
	public static boolean HBrecvd = false;
	NodeTimer timer;
	protected static Logger logger = (Logger) LoggerFactory.getLogger("Follower");
	Thread followerThread = null;
	private static Follower INSTANCE = null;
	TimerRoutine getTimer;
	public static int lastVotedTerm = 0;
	MessageMongoDB mongoDB;

	/********************************************************************************/
	/* Constructor */
	/********************************************************************************/
	private Follower() {
		mongoDB.getInstance();

	}

	/********************************************************************************/
	/* Get Instance of Follower to ensure single instance!! 						  */
	/********************************************************************************/
	public static Follower getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new Follower();

		}
		return INSTANCE;
	}

	@Override
	/********************************************************************************/
	/* Starting Follower Thread!! 												  */
	/********************************************************************************/
	public void run() {
		logger.info("Follower started");
		initFollower();

		followerThread = new Thread() {
			public void run() {
				while (running) {
					while (NodeState.getInstance().getState() == NodeState.FOLLOWER) {
					}
				}

			}
		};

		followerThread.start();
	}

	/********************************************************************************/
	/* Initialising Follower State */
	/********************************************************************************/

	private void initFollower() {
		timer = new NodeTimer();

		timer.schedule(new Runnable() {
			@Override
			public void run() {
				NodeState.getInstance().setState(NodeState.CANDIDATE);
			}
		}, getTimer.getElectionTimeout());

	}

	/********************************************************************************/
	/* Handling Voting Requests */
	/********************************************************************************/

	@Override
	public void handleRequestVote(WorkMessage workMessage) {
		WorkMessage voteResponse;
		System.out.println("Follower : wm term: " + workMessage.getVoteRPCPacket().getRequestVote().getTerm());
		System.out.println("Follower:  term : " + NodeState.currentTerm);
	
		int newTerm = workMessage.getVoteRPCPacket().getRequestVote().getTerm();
		
		if (newTerm > NodeState.currentTerm && newTerm > lastVotedTerm ) {
			lastVotedTerm = newTerm;
			voteResponse = MessageBuilder.prepareResponseVote(ResponseVote.IsVoteGranted.YES);
		} else
			voteResponse = MessageBuilder.prepareResponseVote(ResponseVote.IsVoteGranted.NO);
		
		sendResponseVote(voteResponse, workMessage);

	}

	public static void sendResponseVote(WorkMessage voteResponse, WorkMessage wm) {
		for (Map.Entry<Integer, RemoteNode> entry : NodeMonitor.getInstance().getNodeMap().entrySet()) {
			if (entry.getValue().isActive() && entry.getValue().getChannel() != null) {
				if (entry.getValue().getNodeConf().getNodeId() == wm.getVoteRPCPacket().getRequestVote().getCandidateId()) {
					ChannelFuture cf = entry.getValue().getChannel().writeAndFlush(voteResponse);
					if (cf.isDone() && !cf.isSuccess()) {
						System.out.println("Fail to send vote response message to candidate");
					}
				}
			}
		}
	}

	/********************************************************************************/
	/* Handling HeartBeat Requests */
	/********************************************************************************/

	@Override
	public void handleHeartBeat(WorkMessage wm) {
		/*
		 * If Current term is less than New term recieved in HB Packet, Update current
		 * Term
		 */
		if (NodeState.currentTerm < wm.getHeartBeatPacket().getHeartbeat().getTerm())
			NodeState.currentTerm = wm.getHeartBeatPacket().getHeartbeat().getTerm();

		/*
		 * If Current term is greater than New term recieved in HB Packet, it implies
		 * the heartbeat
		 */
		/* is from incorrect leader and hence ignore */
		if (NodeState.currentTerm > wm.getHeartBeatPacket().getHeartbeat().getTerm()) {
			logger.info("Invalid Heartbeat from previous Invalid Term recieved");
		} else { /* Valid Hearbeat and Valid term so reschedule the timer */
			logger.info("Heartbeat recieved from  Term" + wm.getHeartBeatPacket().getHeartbeat().getTerm());
			timer.reschedule(getTimer.getElectionTimeout());
		}
	}

	/********************************************************************************/
	/* Handling AppendEntries Requests */
	/********************************************************************************/

	@Override
	public void handleAppendEntries(WorkMessage wm) {
		/*
		 * If valid current term, then update the Databse with the Append Entries packet
		 * to complete replication
		 */
		if (NodeState.currentTerm <= wm.getAppendEntriesPacket().getAppendEntries().getTermid()) {
			mongoDB.storeClientMessagetoDB(wm);
			sendAppendEntiresResponse(wm,"YES");
		}
		else sendAppendEntiresResponse(wm,"NO");
	}

	public void sendAppendEntiresResponse(WorkMessage wm, String response) {
		WorkMessage wmResponse =  MessageBuilder.prepareAppendEntriesResponse(response);
		for (Map.Entry<Integer, RemoteNode> entry : NodeMonitor.getInstance().getNodeMap().entrySet()) {

			if (entry.getValue().isActive()) {
				if (entry.getValue().getNodeConf().getNodeId() == wm.getAppendEntriesPacket().getAppendEntries().getLeaderId()) {
					ChannelFuture cf = entry.getValue().getChannel().writeAndFlush(wmResponse);
					if (cf.isDone() && !cf.isSuccess()) {
						System.out.println("Failed to send AppendEntries Response message to Leader");
					}
					break;
				}
			}
		}
	}

	/********************************************************************************/
	/* Starting Follower Service */
	/********************************************************************************/

//	@Override
//	public void startService(Service service) {
//
//		running = Boolean.TRUE;
//		cthread = new Thread((Follower) service);
//		cthread.start();
//
//	}

	/********************************************************************************/
	/* Stoping Follower Service */
	/********************************************************************************/

	@Override
	public void stopService() {
		running = Boolean.FALSE;
	}

}
