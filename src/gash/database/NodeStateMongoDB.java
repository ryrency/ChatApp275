package gash.database;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import gash.router.container.NodeConf;
import gash.router.server.raft.RaftNode;

import org.bson.Document;
import org.bson.conversions.Bson;

import raft.proto.Internal.LogEntry;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
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
		Logger.getGlobal().info("connecting to mongodb for node states");
		NodeConf conf = RaftNode.getInstance().getState().getNodeConf();
		mongoClient = new MongoClient(conf.getHost(), conf.getMongoPort());
		database = mongoClient.getDatabase(DB_NAME);
		logCollection = database.getCollection(LOG_COLLECTION_NAME);
		nodeStateCollection = database.getCollection(NODE_STATE_COLLECTION_NAME);
		
		logCollection.drop();
		nodeStateCollection.drop();
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
	public synchronized boolean saveNodeState(RaftNode.State state) {
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
	
	public synchronized boolean restoreNodeState(RaftNode.State state) {
		try {
			nodeStateDocument = nodeStateCollection.find().first();
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		if (nodeStateDocument == null) return false;
		Logger.getGlobal().info("recovered node state" + nodeStateDocument.toJson());
		
		state.setCurrentTerm(nodeStateDocument.getInteger(CURRENT_TERM, 0));
		state.setLastVotedTerm(nodeStateDocument.getInteger(LAST_VOTED_TERM, 0));
		state.setLastApplied(nodeStateDocument.getInteger(LAST_APPLIED, 0));
		
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
	
	public LogEntry getLogEntry(int logIndex) {
		try {
			Bson filter = Filters.eq(LOG_INDEX, logIndex);
			Document logDocument = (Document)logCollection.find(filter).first();
			return mapDocumentToLogEntry(logDocument);
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
	
	private Document mapLogEntryToDocument(LogEntry entry) {
		if (entry == null) return null;
		Document document = new Document();
		document.put(LOG_TERM, entry.getTerm());
		document.put(LOG_INDEX, entry.getIndex());
		document.put(LOG_COMMAND, entry.getCommand());
		return document;
	}
	
	public List<LogEntry> getLogEntriesFromIndex(int fromIndex) {
		List<LogEntry> entries = new ArrayList<LogEntry>();
		
		try {
			FindIterable<Document> documents = logCollection.find(Filters.gte(LOG_INDEX, fromIndex));
			Iterator<Document> iterator = documents.iterator();
			while (iterator.hasNext()) {
				Document document = iterator.next();
				LogEntry entry = mapDocumentToLogEntry(document);
				entries.add(entry);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		} 
		
		return entries;
	}
	
	public void deleteLogEntries(int fromIndex) {
		try {
			logCollection.deleteMany(Filters.gte(LOG_INDEX, fromIndex));
		} catch (Exception ex) {
			ex.printStackTrace();
		} 
	}
	
	public void commitLogEntries(List<LogEntry> entries) {
		try {
			List<Document> documents = new ArrayList<Document>();
			for (LogEntry entry : entries) documents.add(mapLogEntryToDocument(entry));
			if (documents.size() > 0) logCollection.insertMany(documents);
		} catch (Exception ex) {
			ex.printStackTrace();
		} 
	}
	
}
