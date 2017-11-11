package gash.database;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.Date;

import org.bson.Document;
import com.mongodb.client.model.Filters;




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
	 
	
	

}
