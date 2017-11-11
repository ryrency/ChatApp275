package gash.router.server.raft;

import java.util.TimerTask;

public class NodeTask extends TimerTask{

	Runnable task = null;
	
	NodeTask(Runnable task) {
		this.task = task;
	}
	
	@Override
	public void run() {
		task.run();
	}

}
