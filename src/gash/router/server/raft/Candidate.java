package gash.router.server.raft;

import io.netty.channel.ChannelFuture;
import raft.proto.Work;
import raft.proto.AppendEntries.AppendEntriesPacket;
import raft.proto.AppendEntries.AppendEntriesResponse;
import raft.proto.AppendEntries.AppendEntriesResponse.IsUpdated;
import raft.proto.HeartBeat.HeartBeatPacket;
//	import raft.proto.HeartBeat.HeartBeatResponse;
import raft.proto.Vote.ResponseVote;
import raft.proto.Vote.VotePacket;
import raft.proto.Work.WorkMessage;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.NodeMonitor;
import gash.router.server.RemoteNode;

//	import server.db.DatabaseService;
//	import server.edges.EdgeInfo;
//	
public class Candidate extends Service implements Runnable {

	/********************************************************************************/
	/* Initialisations */
	/********************************************************************************/
	private static Candidate INSTANCE = null;
	private int numberOfYESResponses;
	private int TotalResponses = 0;
	NodeTimer timer = new NodeTimer();
	protected static Logger logger = (Logger) LoggerFactory.getLogger("CANDIDATE");
	HashMap<Integer, RemoteNode> statMap = new HashMap<Integer, RemoteNode>();

	/********************************************************************************/
	/* Constructor */
	/********************************************************************************/
	private Candidate() {
		// TODO Auto-generated constructor stub
	}

	/********************************************************************************/
	/* Get Instance of Candidate to ensure single instance!! */
	/********************************************************************************/
	public static Candidate getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new Candidate();
		}
		return INSTANCE;
	}

	/********************************************************************************/
	/* Starting Candidate Thread!! */
	/********************************************************************************/
	@Override
	public void run() {
		logger.info("Candiate started");
		startElection();
		while (running) {
			while (NodeState.getInstance().getState() == NodeState.CANDIDATE) {
			}
		}
	}

	/********************************************************************************/
	/* Election Process */
	/********************************************************************************/
	private void startElection() {
		numberOfYESResponses = 0;
		TotalResponses = 0;
		NodeState.currentTerm++;

		logger.info("Candiate:StartElection Current term " + NodeState.currentTerm);

		/* Prepare Vote Request Packet to send to Followers(all other active nodes */
		WorkMessage workMessage = MessageBuilder.prepareRequestVote();
		sendRequestVote();

		/*--------------------------------------------------------------------------------------------------------------------*/
		/*
		 * Once request for votes are sent out, wait for a random time and once the
		 * timer goes off check for the no of responses.
		 */
		/*
		 * If, No of Yes Responses is more than 1/2 of the total responses, declare
		 * yourself as a leader or else become follower
		 */
		/*--------------------------------------------------------------------------------------------------------------------*/
		timer = new NodeTimer();
		timer.schedule(new Runnable() {
			@Override
			public void run() {

				if (isWinner()) {
					logger.info(NodeMonitor.getInstance().getNodeConf().getNodeId() + " has won the election.");
					NodeState.getInstance().setState(NodeState.LEADER);
				} else {
					logger.info(NodeMonitor.getInstance().getNodeConf().getNodeId() + " has lost the election.");
					NodeState.getInstance().setState(NodeState.FOLLOWER);
				}
			}

			private Boolean isWinner() {

				logger.info("Total number of responses = " + TotalResponses);
				logger.info("Total number of YES responses = " + numberOfYESResponses);

				if ((numberOfYESResponses + 1) > (TotalResponses + 1) / 2) {
					return Boolean.TRUE;
				}
				return Boolean.FALSE;

			}
		}, TimerRoutine.getFixedTimeout());

	}

	/*------------------------------------------*/
	/* Send Requests for Vote to all Active Nodes */
	/*------------------------------------------*/
	@Override
	public void sendRequestVote() {
		WorkMessage voteRequest = MessageBuilder.prepareRequestVote();

<<<<<<< HEAD
			logger.info(
					"Vote 'YES' is granted from Node Id " + workMessage.getVoteRPCPacket().getResponseVote().getTerm());
			numberOfYESResponses++;

		} else {
			logger.info(
					"Vote 'NO' is granted from Node Id " + workMessage.getVoteRPCPacket().getResponseVote().getTerm());
		}

	}

	// NEED TO CHECK THE NEED FOR
	// THIS**************************************************
	@Override
	public void handleRequestVote(WorkMessage workMessage) {
		WorkMessage voteRequest;
//		if (workMessage.getVoteRPCPacket().getRequestVote().getTimeStampOnLatestUpdate() < NodeState
//				.getTimeStampOnLatestUpdate()) {
//			voteRequest = MessageBuilder.prepareResponseVote(ResponseVote.IsVoteGranted.NO);
//
//		}
//		voteRequest = MessageBuilder.prepareResponseVote(ResponseVote.IsVoteGranted.YES);
		for (RemoteNode ts : this.statMap.values()) {
=======
		for (TopologyStat ts : this.statMap.values()) {
>>>>>>> 35298aa28763a321c7131143ec06deb35a011acf
			if (ts.isActive() && ts.getChannel() != null) {
				logger.info("Sent VoteRequestRPC to " + ts.getRef());
				ChannelFuture cf = ts.getChannel().writeAndFlush(voteRequest);
				if (cf.isDone() && !cf.isSuccess()) {
					logger.info("Vote request send failed!");
				}
			} else {
				logger.info("Channel not active ,server  is down");
			}
		}
	}

	/*------------------------------------------*/
	/* Calculate Responses from other nodes */
	/*------------------------------------------*/

	@Override
	public void handleResponseVote(WorkMessage workMessage) {
		if (workMessage.getVoteRPCPacketOrBuilder().hasResponseVote()) {
			TotalResponses++;

			if (workMessage.getVoteRPCPacket().getResponseVote().getIsVoteGranted() == ResponseVote.IsVoteGranted.YES) {

				logger.info("Vote 'YES' is granted from Node Id "
						+ workMessage.getVoteRPCPacket().getResponseVote().getTerm());
				numberOfYESResponses++;

			} else {
				logger.info("Vote 'NO' is granted from Node Id "
						+ workMessage.getVoteRPCPacket().getResponseVote().getTerm());
			}
		} else
			logger.info("Cannot vote as I am a CANDIDATE MYSELF");
	}

	/********************************************************************************/
	/* Handle Hearbeat if recieved. It implies anpther leader may be active now!!   */
	/********************************************************************************/
	@Override
	public void handleHeartBeat(WorkMessage wm) {
		if (wm.getHeartBeatPacket().getHeartbeat().getTerm() >= NodeState.currentTerm) {
			logger.info(
					"HeartbeatPacket received from leader :" + wm.getHeartBeatPacket().getHeartbeat().getLeaderId());
			NodeState.getInstance().setState(NodeState.FOLLOWER);
		} else
			logger.info("Heartbeat recieved for prev term.. so ignored!!");

	}
	/********************************************************************************/
	/* Starting Candidate Service 												  */
	/********************************************************************************/
	public void startService(Service service) {
		running = Boolean.TRUE;
		cthread = new Thread((Candidate) service);
		cthread.start();
	}
	
	/********************************************************************************/
	/* Stopping Candidate Service 												  */
	/********************************************************************************/
	public void stopService() {
		timer.cancel();
		running = Boolean.FALSE;

	}

}
