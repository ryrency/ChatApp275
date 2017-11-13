package gash.database;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.Date;

import org.bson.Document;
import com.mongodb.client.model.Filters;

import raft.proto.Work.WorkMessage;




public class MongoDB {
	
	static MongoDB mongoDB = null;
	MongoClient mongoClient = null;
	MongoDatabase database = null;
	MongoCollection<Document> dbCollection = null;
	final static String DB_NAME = "275db";
	final static String COLLECTION_NAME = "testClient";
	
	final static String RECEIVER_ID = "receiverID";
	final static String SENDER_ID = "senderId";
	final static String PAYLOAD = "payload";
	final static String TIMESTAMP = "timestamp";
	final static String UNIXTIMESTAMP = "UnixTimeStamp";
	final static String MESSAGETYPE = "MessageType";
	final static String STATUS = "Status";
	final static String TIMESTAMPONLATESTUPDATE = "TimeStampOnLatestUpdate";
	final static String TERMID = "TermID";
	
	

	private MongoDB() {
		// TODO Auto-generated constructor stub
		try {
		mongoClient = new MongoClient();
		database = mongoClient.getDatabase(DB_NAME);
		dbCollection = database.getCollection(COLLECTION_NAME);
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	public static MongoDB getInstance() {
		if(mongoDB == null) {
			mongoDB = new MongoDB();
		}
		return mongoDB;
	}
	
	public FindIterable<Document> get(String key) {
		System.out.println("***MongoDB*** fn:get***");
		 FindIterable<Document> result = null;
		 try {
			 result = dbCollection.find(Filters.eq(RECEIVER_ID,key));
		 }
		 catch(Exception e) {
			 e.printStackTrace();
			 return null;
		 }
		 finally {
			
		 }
		 return result;
		 
	 }
	
	public FindIterable<Document> getNewEntries(Date staleTimeStamp){
		System.out.println("***MongoDB*** fn:getnewEntries***");
		FindIterable<Document> result = null;
		try {
			result = dbCollection.find(Filters.eq(TIMESTAMP,staleTimeStamp));
		}
		catch(Exception ex) {ex.printStackTrace();return null;}
		finally {}
		return result;
	}
	
	public FindIterable<Document> getAllEntries(){
		System.out.println("***MongoDB*** fn:getAllEntries***");
		FindIterable<Document> result = null;
		try {
			result = dbCollection.find();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return null;
		}
		finally {
			
		}
		return result;
	}
	
	public boolean post(String senderId,String receiverId,String payload, Date msgTimeStamp) {
		System.out.println("***MongoDB*** fn:post***");
		try {
			Document document = new Document();
			document.put(SENDER_ID, senderId);
			document.append(RECEIVER_ID, receiverId).append(PAYLOAD, payload).append(TIMESTAMP, msgTimeStamp);
			dbCollection.insertOne(document);
			return true;
		}
		catch(Exception e) {
			e.printStackTrace();
			return false;
		}
		finally {}
	}
	
	public boolean delete(String senderId) {
		System.out.println("***MongoDB*** fn:delete***");
		try {
			dbCollection.deleteMany(Filters.eq(SENDER_ID,senderId));
			return true;
		}
		catch(Exception ex) {ex.printStackTrace();
		return false;}
		finally {}
		
	}
	
	public boolean closeDatabaseConnection() {
		System.out.println("***MongoDB*** fn:closeDatabaseConnection***");
		try {
			mongoClient.close();
			return true;
		}
		catch(Exception e) {
			e.printStackTrace();
			return false;
		}
		finally {}
	}
	
	public boolean storeClientMessagetoDB(WorkMessage workMessage) {
		System.out.println("***MongoDB*** fn:storeClientMessagetoDB");
		try {
			Document document = new Document();
			document.put(UNIXTIMESTAMP, workMessage.getUnixTimeStamp());
			document.append(MESSAGETYPE,workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getType());
			document.append(TERMID,workMessage.getAppendEntriesPacket().getAppendEntries() );
			document.append(SENDER_ID,workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getSender() );
			document.append(RECEIVER_ID,workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getTo() );
			document.append(PAYLOAD,workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getPayload() );
			document.append(TIMESTAMP,workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getTimestamp() );
			document.append(STATUS,workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getStatus() );
			document.append(TIMESTAMPONLATESTUPDATE,workMessage.getAppendEntriesPacket().getAppendEntries().getTimeStampOnLatestUpdate() );
			dbCollection.insertOne(document);
	
			return true;
		}
		catch(Exception e) {e.printStackTrace();
		return false;}
		finally {}
	}
	 
	
	

}
