package gash.router.server.raft;

import io.netty.channel.ChannelFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import raft.proto.Internal.InternalPacket;
import raft.proto.Internal.LogEntry;
import raft.proto.Internal.VoteRequest;
import raft.proto.Internal.VoteResponse;
import gash.database.NodeStateMongoDB;
import gash.router.container.NodeConf;
import gash.router.container.RoutingConf;
import gash.router.discovery.DiscoveryServer;
import gash.router.server.NodeMonitor;
import gash.router.server.RemoteNode;


//todo: all nodes must be present in remote nodes so that we know how many votes to expect
//todo: whenever a request/response is received, do the term check and become follower if needed
//todo: update last log entry whenever a new log is added to the db
//todo: should monitor remove channels which have already been removed?
//todo: should we count votes only for active nodes? or all the nodes even if they are down?
//todo: add mongodb ports into node conf

public class RaftNode {
	
	private static final int DEFAULT_LOG_TERM = 1;
	private static final int DEFAULT_LOG_INDEX = 1;
	
	private static RaftNode instance;
	
	public static RaftNode getInstance() {
		if (instance == null) instance = new RaftNode();
		return instance;
	}
	
	private enum NodeType {
		Follower,
		Candidate,
		Leader
	}
	
	private NodeType nodeType;
	private State state;
	private LogEntry lastLogEntry = null;
	private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(3);
	
	public NodeType getNodeType() {
		return nodeType;
	}
	
	private RaftNode() {
		
	}
	
	public void init(RoutingConf conf, NodeConf nodeConf) {
		state = new State(conf, nodeConf);
		restoreState();
		setNodeType(NodeType.Follower);
	}
	
	public State getState() {
		return state;
	}
	
	/********************************************************************************/
	/* Switch roles */
	/********************************************************************************/
	private synchronized void setNodeType(NodeType type) {
		if (type != nodeType) {
			nodeType = type;
			if (type == NodeType.Follower) {
				stopCandidate();
				stopLeader();
				startFollower();
			} else if (type == NodeType.Candidate) {
				stopFollower();
				stopLeader();
				startCandidate();
			} else {
				stopFollower();
				stopCandidate();
				startLeader();
			}
		}
	}
	
	private void startFollower() {
		Logger.getGlobal().info("Starting as follower");
		rescheduleFollowerTask();
	}
	
	private void rescheduleFollowerTask() {
		executor.remove(followerTask);
		executor.schedule(
				followerTask, 
				TimerRoutine.getFollowerTimeOut(), 
				TimeUnit.MILLISECONDS
		);
	}
	
	private void stopFollower() {
		executor.remove(followerTask);
	}
	
	private void startCandidate() {
		Logger.getGlobal().info("Starting as candidate");
		executor.submit(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				requestVotes();
			}
		});
	}
	
	private void stopCandidate() {
		executor.remove(candidateTask);
	}
	
	private void startLeader() {
		Logger.getGlobal().info("Starting as leader");
		executor.submit(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				sendHeartbeat();
			}
		});
	}
	
	private void stopLeader() {
		executor.remove(leaderTask);
	}
 	
	/********************************************************************************/
	/* Saving and restoring states */
	/********************************************************************************/
	private void saveState() {
		NodeStateMongoDB.getInstance().saveNodeState(state);
	}
	
	private void restoreState() {
		NodeStateMongoDB.getInstance().restoreNodeState(state);
	}
	
	/********************************************************************************/
	/* Ask for votes Requests */
	/********************************************************************************/
	private synchronized void requestVotes() {
		Logger.getGlobal().info("starting to request votes");
		// start election, vote to self
		voteToSelf();
		
		// request votes from all servers
		InternalPacket packet = prepareVoteRequest();
		broadcastVoteRequestPacket(packet);
		
		// check if it has already won the election
		if (isWinner()) {
			setNodeType(NodeType.Leader);
		} else {
			executor.schedule(candidateTask, TimerRoutine.getFixedTimeout(), TimeUnit.MILLISECONDS);
		}
	}
	
	private void voteToSelf() {
		state.setVotesExpected(1);
		state.setVotesReceived(1);
		state.setYesVotes(1);
		state.incrementCurrentTerm();
		state.setLastVotedTerm(state.getCurrentTerm());
		saveState();
	}
	
	private InternalPacket prepareVoteRequest() {
		// get last log entry from db
		if (lastLogEntry == null) lastLogEntry = NodeStateMongoDB.getInstance().getLastLogEntry();
		int lastLogTerm = lastLogEntry != null ? lastLogEntry.getTerm() : DEFAULT_LOG_TERM;
		int lastLogIndex = lastLogEntry != null ? lastLogEntry.getIndex() : DEFAULT_LOG_INDEX;
				
		//prepare vote request
		VoteRequest request = 
				VoteRequest
				.newBuilder()
				.setTerm(state.getCurrentTerm())
				.setCandidateId(state.getNodeConf().getNodeId())
				.setLastLogTerm(lastLogTerm)
				.setLastLogIndex(lastLogIndex)
				.build();
				
		InternalPacket packet = 
				InternalPacket
				.newBuilder()
				.setVoteRequest(request)
				.build();
				
		
		return packet;
	}
	
	private synchronized void broadcastVoteRequestPacket(InternalPacket packet) {
		
		for (RemoteNode rm : NodeMonitor.getInstance().getNodeMap().values()) {
			state.incrementVotesExpected();
			
			if (rm.isActive() && rm.getChannel() != null) {
				Logger.getGlobal().info("Sent VoteRequestRPC to " + rm.getNodeConf().getNodeId());
				ChannelFuture cf = rm.getChannel().writeAndFlush(packet);
			} else {
				Logger.getGlobal().info("Channel not active ,server  is down");
			}
		}
	}
	
	/********************************************************************************/
	/* Respond to vote requests */
	/********************************************************************************/
	public synchronized InternalPacket handleVoteRequest(VoteRequest request) {
		
		boolean isVoteGranted = isVoteGranted(request.getTerm());
		
		VoteResponse response = 
				VoteResponse
				.newBuilder()
				.setVoteGranted(isVoteGranted)
				.setTerm(state.getCurrentTerm())
				.build();
		
		InternalPacket packet = 
				InternalPacket
				.newBuilder()
				.setVoteResponse(response)
				.build();
		
		return packet;
	}
	
	private boolean isVoteGranted(int candidateTerm) {
		boolean granted = 
				nodeType == NodeType.Follower && 
				candidateTerm > state.getCurrentTerm() && 
				candidateTerm > state.getLastVotedTerm();
				
		if (granted) {
			state.setLastVotedTerm(candidateTerm);
			saveState();
		}
		return granted;
	}
	
	/********************************************************************************/
	/* Handling Voting Responses */
	/********************************************************************************/
	public synchronized void handleVoteResponse(VoteResponse response) {
		Logger.getGlobal().info("vote received: " + response.toString());
		if (response.getTerm() <= state.getCurrentTerm()) {
			state.incrementVotesReceived();
			if (response.getVoteGranted()) state.incrementYesVotes();
			
			//todo: also check election result after the voting timeout
			if (state.getVotesExpected() == state.getVotesReceived()) checkElectionResult();
		} else {
			setNodeType(NodeType.Follower);
			Logger.getGlobal().info("term outdated, converting to follower");
		}
	}
	
	private synchronized void checkElectionResult() {
		Logger.getGlobal().info("checking election results");
		if (isWinner()) {
			Logger.getGlobal().info(state.getNodeConf().getNodeId() + " has won the election.");
			setNodeType(NodeType.Leader);
		} else {
			Logger.getGlobal().info(state.getNodeConf().getNodeId() + " has lost the election.");
			NodeState.getInstance().setState(NodeState.FOLLOWER);
			setNodeType(NodeType.Follower);
		}
	}
	
	private boolean isWinner() {
		Logger.getGlobal().info("Total number of votes = " + state.getVotesReceived());
		Logger.getGlobal().info("Expected number of votes = " + state.getVotesExpected());
		Logger.getGlobal().info("Total number of YES votes = " + state.getYesVotes());

		if (state.getYesVotes() > state.getVotesExpected()/2) {
			return true;
		}
		
		return false;
	}
	
	/********************************************************************************/
	/* Leader functions: append edtries, send heartbeats */
	/********************************************************************************/
	private void sendHeartbeat() {
		//todo: send heartbeat here
		Logger.getGlobal().info("sending heartbeats");
	}
	
	private synchronized void scheduleHeartBeatSend() {
		executor.schedule(leaderTask, TimerRoutine.getHeartbeatSendDelay(), TimeUnit.MILLISECONDS);
	}
	
	private void startDiscoveryServer() {
		
		DiscoveryServer udpDiscoveryServer = new DiscoveryServer(state.getConf(), state.getNodeConf());
		Thread discoveryThread = new Thread(udpDiscoveryServer);
		discoveryThread.start();
	}
	
	private void stopDiscoveryServer() {
		//todo implement this
	}
	
	/********************************************************************************/
	/* Background tasks for follower, candidate, and leader */
	/********************************************************************************/
	private Runnable followerTask = new Runnable() {
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			//no heart beat received, become candidate
			Logger.getGlobal().info("follower heartbeat timed out");
			setNodeType(NodeType.Candidate);
		}
	};
	
	private Runnable candidateTask = new Runnable() {
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			if (nodeType == NodeType.Candidate) 
				checkElectionResult();
			
		}
	};
	
	private Runnable leaderTask = new Runnable() {
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			sendHeartbeat();
			scheduleHeartBeatSend();
		}
	};
	
	
	/********************************************************************************/
	/* Node state */
	/********************************************************************************/
	
	public class State {
		// persistent variables
		private int currentTerm = 1;
		private int votedFor = -1;
		private int lastApplied = 0;
		
		// volatile variables 
		private RoutingConf conf;
		private NodeConf nodeConf;
		private int commitIndex = 0;
		private int lastVotedTerm;
		
		// leader variables
		private Map<Integer, Integer> nextIndices = new HashMap<Integer, Integer>();
		private Map<Integer, Integer> matchIndices = new HashMap<Integer, Integer>();
		
		// election variables
		private int votesReceived;
		private int votesExpected;
		private int yesVotes;
		
		public int getCurrentTerm() {
			return currentTerm;
		}
		
		public void setCurrentTerm(int currentTerm) {
			this.currentTerm = currentTerm;
		}
		
		public int getLastApplied() {
			return lastApplied;
		}
		
		public void setLastApplied(int lastApplied) {
			this.lastApplied = lastApplied;
		}
		
		public RoutingConf getConf() {
			return conf;
		}
		
		public void setConf(RoutingConf conf) {
			this.conf = conf;
		}
		
		public NodeConf getNodeConf() {
			return nodeConf;
		}
		
		public void setNodeConf(NodeConf conf) {
			this.nodeConf = conf;
		}
		
		public int getCommitIndex() {
			return commitIndex;
		}
		
		public void setCommitIndex(int commitIndex) {
			this.commitIndex = commitIndex;
		}
		
		public int getLastVotedTerm() {
			return lastVotedTerm;
		}
		
		public void setLastVotedTerm(int term) {
			lastVotedTerm = term;
		}
		
		public State(RoutingConf conf, NodeConf nodeConf) {
			this.conf = conf;
			this.nodeConf = nodeConf;
		}
		
		public int getVotesReceived() {
			return votesReceived;
		}
		
		public void setVotesReceived(int votesReceived) {
			this.votesReceived = votesReceived;
		}
		
		public int getVotesExpected() {
			return votesExpected;
		}
		
		public void setVotesExpected(int votesExpected) {
			this.votesExpected = votesExpected;
		}
		
		public int getYesVotes() {
			return yesVotes;
		}
		
		public void setYesVotes(int yesVotes) {
			this.yesVotes = yesVotes;
		}
		
		public synchronized void incrementCurrentTerm() {
			currentTerm++;
		}
		
		public void incrementVotesExpected() {
			votesExpected++;
		}
		
		public void incrementVotesReceived() {
			votesReceived++;
		}
		
		public void incrementYesVotes() {
			yesVotes++;
		}
	}

}
