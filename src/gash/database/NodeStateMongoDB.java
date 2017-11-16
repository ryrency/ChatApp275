package gash.database;

import java.util.logging.Logger;

import gash.router.server.raft.RaftNode;

import org.bson.Document;
import org.bson.conversions.Bson;

import raft.proto.Internal.LogEntry;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

public class NodeStateMongoDB {
	final static String DB_NAME = "275db";
	final static String NODE_STATE_COLLECTION_NAME = "node_state";
	final static String LOG_COLLECTION_NAME = "logs";
	
	final static String CURRENT_TERM = "currentTerm";
	final static String LAST_VOTED_TERM = "lastVotedTerm";
	final static String LAST_APPLIED = "lastApplied";
	
	final static String LOG_TERM = "term";
	final static String LOG_INDEX = "index";
	final static String LOG_COMMAND = "command";
	
	private static final int ID_IDX = 1;
	
	private static NodeStateMongoDB instance = null;
	
	MongoClient mongoClient = null;
	MongoDatabase database = null;
	MongoCollection<Document> logCollection = null;
	MongoCollection<Document> nodeStateCollection = null;
	
	Document nodeStateDocument = null;
	
	private NodeStateMongoDB() {
		mongoClient = new MongoClient();
		database = mongoClient.getDatabase(DB_NAME);
		logCollection = database.getCollection(LOG_COLLECTION_NAME);
		nodeStateCollection = database.getCollection(NODE_STATE_COLLECTION_NAME);
	}
	
	public static NodeStateMongoDB getInstance() {
		if (instance == null) {
			instance = new NodeStateMongoDB();
		}
		return instance;
	}
	
	/********************************************************************************/
	/* Persists node state in mongodb */
	/********************************************************************************/
	
	public boolean saveNodeState(RaftNode.State state) {
		if(nodeStateDocument == null) {
			nodeStateDocument = new Document();
			nodeStateDocument.put("_id", ID_IDX);
		}
		
		nodeStateDocument.put(CURRENT_TERM, state.getCurrentTerm());
		nodeStateDocument.put(LAST_VOTED_TERM, state.getLastVotedTerm());
		nodeStateDocument.put(LAST_APPLIED, state.getLastApplied());
		
		try {
			Bson filter = Filters.eq("_id", ID_IDX);
			UpdateOptions options = new UpdateOptions().upsert(true);
			nodeStateCollection.replaceOne(filter, nodeStateDocument, options);
			Logger.getGlobal().info("node state saved");
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public boolean restoreNodeState(RaftNode.State state) {
		try {
			nodeStateDocument = nodeStateCollection.find().first();
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		if (nodeStateDocument == null) return false;
		Logger.getGlobal().info("recovered node state" + nodeStateDocument.toJson());
		
		state.setCurrentTerm(nodeStateDocument.getInteger(CURRENT_TERM));
		state.setLastVotedTerm(nodeStateDocument.getInteger(LAST_VOTED_TERM));
		state.setLastApplied(nodeStateDocument.getInteger(LAST_APPLIED));
		
		return true;
	}
	
	/********************************************************************************/
	/* Persist logs in mongodb */
	/********************************************************************************/
	public LogEntry getLastLogEntry() {
		try {
			Document lastLogDocument = (Document)logCollection.find().sort(new BasicDBObject("_id",-1)).first();
			return mapDocumentToLogEntry(lastLogDocument);
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private LogEntry mapDocumentToLogEntry(Document lastLogDocument) {
		if (lastLogDocument == null) return null;
		LogEntry entry = LogEntry.
				newBuilder()
				.setTerm(lastLogDocument.getInteger(LOG_TERM))
				.setIndex(lastLogDocument.getInteger(LOG_INDEX))
				.setCommand(lastLogDocument.getString(LOG_COMMAND))
				.build();
		
		return entry;
	}
	
	
}
