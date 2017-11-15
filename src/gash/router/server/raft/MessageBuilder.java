package gash.router.server.raft;

import com.sun.corba.se.impl.ior.NewObjectKeyTemplateBase;
import gash.router.server.NodeMonitor;
import raft.proto.AppendEntries;
import raft.proto.AppendEntries.*;
import raft.proto.AppendEntries.AppendEntriesResponse.IsUpdated;
import raft.proto.HeartBeat.*;
import raft.proto.Vote.*;
import raft.proto.Vote.ResponseVote.IsVoteGranted;
import raft.proto.Work.WorkMessage;
import routing.Pipe.Route;
import raft.proto.InternalNodeAdd.*;

public class MessageBuilder {

	
	public static WorkMessage prepareRequestVote() {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

		RequestVote.Builder requestVote = RequestVote.newBuilder();
		
		//requestVote.setTerm(NodeMonitor.getInstance().getNodeConf().getNodeId());
		requestVote.setCandidateId(NodeMonitor.getInstance().getNodeConf().getNodeId());
		requestVote.setTerm(NodeState.currentTerm);
		System.out.println("MssageBuilder: Current term "+NodeState.currentTerm);

		requestVote.setTimeStampOnLatestUpdate(NodeState.getTimeStampOnLatestUpdate());
		// requestVoteRPC.setTimeStampOnLatestUpdate(DatabaseService.getInstance().getDb().getCurrentTimeStamp());

		VotePacket.Builder voteRPCPacket = VotePacket.newBuilder();
		voteRPCPacket.setUnixTimestamp(TimerRoutine.getCurrentUnixTimeStamp());
		voteRPCPacket.setRequestVote(requestVote);
		
		work.setVoteRPCPacket(voteRPCPacket);

		return work.build();
	}

	public static WorkMessage prepareAppendEntriesResponse(String response) {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

		AppendEntriesPacket.Builder appendEntriesPacket = AppendEntriesPacket.newBuilder();
		appendEntriesPacket.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

		AppendEntriesResponse.Builder appendEntriesResponse = AppendEntriesResponse.newBuilder();
		if(response == "YES")
			appendEntriesResponse.setIsUpdated(IsUpdated.YES);
		else
			appendEntriesResponse.setIsUpdated(IsUpdated.NO);
		appendEntriesPacket.setAppendEntriesResponse(appendEntriesResponse);

		work.setAppendEntriesPacket(appendEntriesPacket);

		return work.build();

	}



	public static WorkMessage prepareHeartBeat() {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

		HeartBeatMsg.Builder heartbeat = HeartBeatMsg.newBuilder();
		heartbeat.setLeaderId(NodeMonitor.getInstance().getNodeConf().getNodeId());
		heartbeat.setTerm(NodeState.currentTerm);
		// Optional

		heartbeat.setTimeStampOnLatestUpdate(NodeState.getTimeStampOnLatestUpdate());

		// heartbeat.setTimeStampOnLatestUpdate(DatabaseService.getInstance().getDb().getCurrentTimeStamp());
		HeartBeatPacket.Builder heartBeatPacket = HeartBeatPacket.newBuilder();
		heartBeatPacket.setUnixTimestamp(TimerRoutine.getCurrentUnixTimeStamp());
		heartBeatPacket.setHeartbeat(heartbeat);

		work.setHeartBeatPacket(heartBeatPacket);

		return work.build();
	}

	public static WorkMessage prepareResponseVote(IsVoteGranted decision) {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

		VotePacket.Builder voteRPCPacket = VotePacket.newBuilder();
		voteRPCPacket.setUnixTimestamp(TimerRoutine.getCurrentUnixTimeStamp());

		ResponseVote.Builder responseVoteRPC = ResponseVote.newBuilder();
		responseVoteRPC.setTerm(NodeState.currentTerm);
		responseVoteRPC.setIsVoteGranted(decision);

		voteRPCPacket.setResponseVote(responseVoteRPC);

		work.setVoteRPCPacket(voteRPCPacket);

		return work.build();
	}
	
	public static WorkMessage prepareInternalNodeAddRequest(int id, String host, int port) {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());
		
		InternalNodeAddPacket.Builder internalNodeAddPacket = InternalNodeAddPacket.newBuilder();
		
		InternalNodeAddRequest.Builder internalNodeAddRequest = InternalNodeAddRequest.newBuilder();
		internalNodeAddRequest.setId(id);
		internalNodeAddRequest.setHost(host);
		internalNodeAddRequest.setPort(port);
		
		internalNodeAddPacket.setInternalNodeAddRequest(internalNodeAddRequest);
		work.setInternalNodeAddPacket(internalNodeAddPacket);
		return work.build();
		
	}
	
	public static WorkMessage prepareAppendEntriesPacket(Route clientMsg, String timestamp) {

        WorkMessage.Builder work = WorkMessage.newBuilder();
        work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

        AppendEntriesPacket.Builder appendEntriesPacket = AppendEntriesPacket.newBuilder();
        appendEntriesPacket.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());
        AppendEntries.ClientMessage.Builder clientMsgBuild = AppendEntries.ClientMessage.newBuilder();


        AppendEntriesMsg.Builder appendEntries = AppendEntriesMsg.newBuilder();
        appendEntries.setTimeStampOnLatestUpdate(timestamp);
        appendEntries.setLeaderId(NodeMonitor.getInstance().getNodeConf().getNodeId());
        appendEntries.setTermid(NodeState.getInstance().currentTerm);
        

        clientMsgBuild.setSender(clientMsg.getMessage().getSenderId());
        clientMsgBuild.setPayload(clientMsg.getMessage().getPayload());
        clientMsgBuild.setTo(clientMsg.getMessage().getReceiverId());
        clientMsgBuild.setTimestamp(timestamp);
        
        clientMsgBuild.setType(clientMsg.getMessage().getType().getNumber());
        clientMsgBuild.setStatus(clientMsg.getMessage().getStatus().getNumber());
        appendEntries.setMessage(clientMsgBuild);
        
        appendEntriesPacket.setAppendEntries(appendEntries);
        
        work.setAppendEntriesPacket(appendEntriesPacket);

        return work.build();

    }
}
