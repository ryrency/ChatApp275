package gash.router.server.raft;

import java.util.Random;

public class TimerRoutine {

	public static long getCurrentUnixTimeStamp() {
		long unixTime = System.currentTimeMillis() / 1000L;
		return unixTime;

	}

	public static long getElectionTimeout() {
		Random random = new Random();
		long variableTimeout = random.nextInt((int) ((5000L - 1000L) + 1L));
		long currentTimeout = 28000L + (long) variableTimeout;
		return currentTimeout;

	}
	
	public static long getFollowerTimeOut() {
		// 2s (fixed delay) + 1s(variable delay)
		Random random = new Random();
		long variableTimeout = random.nextInt((int)(1000L));
		long currentTimeout = 2000L + (long) variableTimeout;
		return currentTimeout;
	}
	
	public static long getHeartbeatSendDelay() {
		return 1000L;
	}

	int randomWithRange(int min, int max) {
		int range = (max - min) + 1;
		return (int) (Math.random() * range) + min;
	}

	public static long getFixedTimeout() {
		return 10000L;
	}
}
