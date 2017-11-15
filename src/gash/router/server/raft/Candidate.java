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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.NodeMonitor;
import gash.router.server.RemoteNode;

//	import server.db.DatabaseService;
//	import server.edges.EdgeInfo;
//	
public class Candidate extends Service implements Runnable {
	
	private ScheduledThreadPoolExecutor executor;

	/********************************************************************************/
	/* Initialisations */
	/********************************************************************************/
	private static Candidate INSTANCE = null;
	private int yesResponses;
	private int totalResponses = 0;
	private int expectedResponses = 0;
	NodeTimer timer = new NodeTimer();
	protected static Logger logger = (Logger) LoggerFactory.getLogger("CANDIDATE");

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
		startElection();
	}

	/********************************************************************************/
	/* Election Process */
	/********************************************************************************/
	private void startElection() {
		logger.info("Candiate started");
		
		yesResponses = 0;
		totalResponses = 0;
		expectedResponses = 0;
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
		if (expectedResponses == 0) {
			checkElectionResult();
		} else {
			executor.schedule(new Runnable() {
				@Override
				public void run() {
						if (running) checkElectionResult();
				}}, TimerRoutine.getFixedTimeout(), TimeUnit.MILLISECONDS);
		}

	}

	/*------------------------------------------*/
	/* Send Requests for Vote to all Active Nodes */
	/*------------------------------------------*/
	@Override
	public void sendRequestVote() {
		WorkMessage voteRequest = MessageBuilder.prepareRequestVote();

		for (RemoteNode ts : NodeMonitor.getInstance().getStatMap().values()) {
			if (ts.isActive() && ts.getChannel() != null) {
				logger.info("Sent VoteRequestRPC to " + ts.getRef());
				ChannelFuture cf = ts.getChannel().writeAndFlush(voteRequest);
				expectedResponses++;
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
			totalResponses++;

			if (workMessage.getVoteRPCPacket().getResponseVote().getIsVoteGranted() == ResponseVote.IsVoteGranted.YES) {

				logger.info("Vote 'YES' is granted from Node Id " + workMessage.getVoteRPCPacket().getResponseVote().getTerm());
				yesResponses++;

			} else {
				logger.info("Vote 'NO' is granted from Node Id " + workMessage.getVoteRPCPacket().getResponseVote().getTerm());
			}
			
			if(totalResponses == expectedResponses) checkElectionResult(); 
			
		} else
			logger.info("Cannot vote as I am a CANDIDATE MYSELF");
	}
	
	private void checkElectionResult() {
		if (isWinner()) {
			logger.info(NodeMonitor.getInstance().getNodeConf().getNodeId() + " has won the election.");
			NodeState.getInstance().setState(NodeState.LEADER);
		} else {
			logger.info(NodeMonitor.getInstance().getNodeConf().getNodeId() + " has lost the election.");
			NodeState.getInstance().setState(NodeState.FOLLOWER);
		}
	}
	
	private boolean isWinner() {
		logger.info("Total number of responses = " + totalResponses);
		logger.info("Expected number of responses = " + expectedResponses);
		logger.info("Total number of YES responses = " + yesResponses);

		if (yesResponses > expectedResponses/2 || expectedResponses == 0) {
			return true;
		}
		
		return false;
	}

	/********************************************************************************/
	/* Handle Hearbeat if recieved. It implies anpther leader may be active now!!   */
	/********************************************************************************/
	@Override
	public void handleHeartBeat(WorkMessage wm) {
		if (wm.getHeartBeatPacket().getHeartbeat().getTerm() >= NodeState.currentTerm) {
			logger.info("HeartbeatPacket received from leader :" + wm.getHeartBeatPacket().getHeartbeat().getLeaderId());
			NodeState.getInstance().setState(NodeState.FOLLOWER);
		} else
			logger.info("Heartbeat recieved for prev term.. so ignored!!");

	}
	/********************************************************************************/
	/* Starting Candidate Service 												  */
	/********************************************************************************/
	public void startService(Service service) {
		running = Boolean.TRUE;
		executor = new ScheduledThreadPoolExecutor(1);
		executor.submit(this);
	}
	
	/********************************************************************************/
	/* Stopping Candidate Service 												  */
	/********************************************************************************/
	public void stopService() {
		running = Boolean.FALSE;
		executor.shutdownNow();
	}

}
