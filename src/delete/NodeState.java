package delete;

import java.sql.Timestamp;

import message.server.NodeMonitor;
import message.server.config.RoutingConf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeState {

	public static final int LEADER = 0;
	public static final int CANDIDATE = 1;
	public static final int FOLLOWER = 2;

	private static int state = FOLLOWER;

	public static int currentTerm = 0;
	
	protected static Logger logger = (Logger) LoggerFactory.getLogger("NODESTATE");

	// private static Timestamp timeStampOnLatestUpdate=null;
	private static Long timeStampOnLatestUpdate = null;

	// private static long noTaskProcessed = 0;
	
	public static RoutingConf conf;

	public static Long getTimeStampOnLatestUpdate() {
		if (timeStampOnLatestUpdate == null) {
			// timeStampOnLatestUpdate = new Timestamp(System.currentTimeMillis());
			timeStampOnLatestUpdate = System.currentTimeMillis();

			// DatabaseService.getInstance().getDb().getCurrentTimeStamp(); -
		}
		return timeStampOnLatestUpdate;
	}

	public static void setTimeStampOnLatestUpdate(Long timeStampOnLatestUpdate) {
		NodeState.timeStampOnLatestUpdate = timeStampOnLatestUpdate;
	}

	// public static void updateTaskCount() {
	// noTaskProcessed++;
	// }
	//
	public static Service getService() {
		return service;
	}

	// public static int getupdatedTaskCount() {
	// return (int)noTaskProcessed;
	// }
	private static Service service;

	private static NodeState instance = null;

	// private ServerState serverState = null;

	private NodeState() {

		// service = Follower.getInstance(); // Service starting as follower

	}

	public static NodeState getInstance() {
		if (instance == null) {
			instance = new NodeState();
		}
		return instance;
	}
	//
	// public void setServerState(ServerState serverState){
	// this.serverState= serverState;
	// }
	//
	// public ServerState getServerState()
	// {
	// return serverState;
	//
	// }

	public synchronized void setState(int newState) {
		state = newState;

		if (newState == NodeState.FOLLOWER) {
			service.stopService();
			service = Follower.getInstance();
			service.startService(service);
		} else if (newState == NodeState.LEADER) {
			logger.info(NodeMonitor.getInstance().getNodeConf().getNodeId() + " is the leader!!.");
			// service = Leader.getInstance();
			service.stopService();
			service = Leader.getInstance();
			service.startService(service);
		} else if (newState == NodeState.CANDIDATE) {
			service.stopService();
			service = Candidate.getInstance();
			service.startService(service);
		}
	}

	public synchronized int getState() {
		return state;
	}
	
	public static RoutingConf getConf() {
		return conf;
	}

	public static void setConf(RoutingConf conf) {
		NodeState.conf = conf;
	}
}
