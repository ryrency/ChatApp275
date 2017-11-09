package gash.router.server;

import java.sql.Timestamp;
import gash.router.server.raft.Follower;

public class NodeState {

	public static final int LEADER = 0;

	public static final int CANDIDATE = 1;

	public static final int FOLLOWER = 2;

	private static int state = 2;
	
	public static int currentTerm = 0;

	private static Timestamp timeStampOnLatestUpdate=null;
	
//	private static long noTaskProcessed = 0;
	
	public static Timestamp getTimeStampOnLatestUpdate() {
		if (timeStampOnLatestUpdate == null) {
			timeStampOnLatestUpdate = new Timestamp(System.currentTimeMillis());
 
//					DatabaseService.getInstance().getDb().getCurrentTimeStamp(); - 
		}
		return timeStampOnLatestUpdate;
	}

	public static void setTimeStampOnLatestUpdate(Timestamp timeStampOnLatestUpdate) {
		NodeState.timeStampOnLatestUpdate = timeStampOnLatestUpdate;
	}

//	public static void updateTaskCount() {
//		noTaskProcessed++;
//	}
//		
//	public static Service getService() {
//		return service;
//	}	
//	
//	public static int getupdatedTaskCount() {
//		return (int)noTaskProcessed;
//	}
//	private static Service service;
	private static Follower service;

	private static NodeState instance = null;
	
//	private  ServerState serverState = null;

	private NodeState() {

		service = Follower.getInstance();

	}

	public static NodeState getInstance() {
		if (instance == null) {
			instance = new NodeState();
		}
		return instance;
	}
//	
//	public void setServerState(ServerState serverState){
//		this.serverState= serverState;
//	}
//	
//	public ServerState getServerState()
//	{
//		return serverState;
//		
//	}

	public synchronized void setState(int newState) {
		state = newState;

		if (newState == NodeState.FOLLOWER) 
			service = Follower.getInstance();
		else if (newState == NodeState.LEADER) 
			service = Leader.getInstance();
		else if (newState == NodeState.CANDIDATE) 
			service = CandidateService.getInstance();
		}
	}

	public synchronized int getState() {
		return state;
	}
}
