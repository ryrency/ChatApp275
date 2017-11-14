package gash.router.server.raft;

import raft.proto.Work.WorkMessage;

public class Service {

	protected volatile Boolean running = Boolean.TRUE;
	static Thread cthread;
	
	public void startService(Service service) {

	}

	public void stopService() {
		// TODO Auto-generated method stub

	}

	public void handleResponseVote(WorkMessage workMessage) {
		// TODO Auto-generated method stub

	}

	public void handleRequestVote(WorkMessage workMessage) {
		// TODO Auto-generated method stub
	}
	
	public void sendRequestVote() {
		
	}
	
	public void sendHeartBeat() {

	}
	

	public void handleHeartBeat(WorkMessage wm) {

	}
	
	public void handleHeartBeatResponse(WorkMessage wm) {

	}

	public void handleAppendEntries(WorkMessage wm) {

	}
	
	public byte[] handleGetMessage(String key) {
		return new byte[1];
	}
	
	public String handlePostMessage(byte[] image, long timestamp) {
		return null;
	}

	public void handlePutMessage(String key, byte[] image, long timestamp) {
		
	}
	
	public void handleDelete(String key) {
		
	}


}
