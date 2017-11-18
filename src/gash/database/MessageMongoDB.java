package gash.database;

import com.google.protobuf.Timestamp;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import gash.router.container.NodeConf;
import gash.router.server.raft.RaftNode;

import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.bson.Document;

import com.mongodb.client.model.Filters;

import raft.proto.Work.WorkMessage;
import routing.Pipe.Message;

public class MessageMongoDB {

	static MessageMongoDB messageMongoDB = null;
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
	final static String READ = "Read";

	private MessageMongoDB() {
		// TODO Auto-generated constructor stub
		try {
			Logger.getGlobal().info("connecting to mongodb for messages");
			NodeConf conf = RaftNode.getInstance().getState().getNodeConf();
			mongoClient = new MongoClient(conf.getHost(), conf.getMongoPort());
			database = mongoClient.getDatabase(DB_NAME);
			dbCollection = database.getCollection(COLLECTION_NAME);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	public static MessageMongoDB getInstance() {
		if (messageMongoDB == null) {
			messageMongoDB = new MessageMongoDB();
		}
		return messageMongoDB;
	}

	public FindIterable<Document> get(String key) {
		System.out.println("***MongoDB*** fn:get***");
		FindIterable<Document> result = null;
		try {
			result = dbCollection.find(Filters.eq(RECEIVER_ID, key));
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {

		}
		return result;

	}

	public FindIterable<Document> getNewEntries(Date staleTimeStamp) {
		System.out.println("***MongoDB*** fn:getnewEntries***");
		FindIterable<Document> result = null;
		try {
			result = dbCollection.find(Filters.eq(TIMESTAMP, staleTimeStamp));
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		} finally {
		}
		return result;
	}

	public FindIterable<Document> getAllEntries() {
		System.out.println("***MongoDB*** fn:getAllEntries***");
		FindIterable<Document> result = null;
		try {
			result = dbCollection.find();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return null;
		} finally {

		}
		return result;
	}

	public boolean delete(String senderId) {
		System.out.println("***MongoDB*** fn:delete***");
		try {
			dbCollection.deleteMany(Filters.eq(SENDER_ID, senderId));
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		} finally {
		}

	}

	public boolean closeDatabaseConnection() {
		System.out.println("***MongoDB*** fn:closeDatabaseConnection***");
		try {
			mongoClient.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
		}
	}

	public boolean storeClientMessagetoDB(WorkMessage workMessage) {
		System.out.println("***MongoDB*** fn:storeClientMessagetoDB");
		try {
			Document document = new Document();
			document.put(UNIXTIMESTAMP, workMessage.getUnixTimeStamp());
			document.append(MESSAGETYPE,
					workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getType());
			document.append(TERMID, workMessage.getAppendEntriesPacket().getAppendEntries().getTermid());
			document.append(SENDER_ID,
					workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getSender());
			document.append(RECEIVER_ID, workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getTo());
			document.append(PAYLOAD, workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getPayload());
			Date date = new Date(workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getTimestamp().replace(".", ""));
			document.append(TIMESTAMP, date);
			document.append(STATUS, workMessage.getAppendEntriesPacket().getAppendEntries().getMessage().getStatus());
			document.append(TIMESTAMPONLATESTUPDATE,
					workMessage.getAppendEntriesPacket().getAppendEntries().getTimeStampOnLatestUpdate());
			document.append(READ, 0);
			dbCollection.insertOne(document);

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
		}
	}
	
	//todo(parag): all fields in messages should be saved
	public boolean commitMessage(Message message) {
		System.out.println("***MongoDB*** fn:storeClientMessagetoDB");
		try {
			Document document = new Document();
			document.append(MESSAGETYPE, message.getType().getNumber());
			document.append(SENDER_ID, message.getSenderId());
			document.append(RECEIVER_ID, message.getReceiverId());
			document.append(PAYLOAD, message.getPayload());
			document.append(TIMESTAMP, message.getTimestamp());
			document.append(STATUS, message.getStatus().getNumber());
			document.append(READ, 0);
			dbCollection.insertOne(document);

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	//todo:parag
	public boolean markMessagesRead(String uname, int lastSeenIndex) {
		System.out.println("***MongoDB*** markMessagesRead");
		try {
			//implement here

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean postData1() {
		System.out.println("***MongoDB*** fn:storeClientMessagetoDB");
		try {
			Document document = new Document();
			document.put(UNIXTIMESTAMP, Timestamp.getDefaultInstance());
			document.append(MESSAGETYPE, 0);
			document.append(TERMID, 2);
			document.append(SENDER_ID, "rency");
			document.append(RECEIVER_ID, "shefali");
			document.append(PAYLOAD, "Hi, this is me");
			Date date1 = new Date();

			Date date = new Date(Long.parseLong(date1.toString()));
			document.append(TIMESTAMP, Timestamp.getDefaultInstance());
			document.append(STATUS, 0);
			document.append(TIMESTAMPONLATESTUPDATE, Timestamp.getDefaultInstance());
			document.append(READ, 0);
			dbCollection.insertOne(document);

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
		}
	}

	// String DATE_FORMAT_NOW = "yyyy-MM-dd";
	// Date date = new Date();
	// SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
	// String stringDate = sdf.format(date );
	// try {
	// Date date2 = sdf.parse(stringDate);
	// } catch(ParseException e){
	// //Exception handling
	// } catch(Exception e){
	// //handle exception
	// }
	//
	public boolean postData2() {
		System.out.println("***MongoDB*** fn:storeClientMessagetoDB");
		try {
			Document document = new Document();
			document.put(UNIXTIMESTAMP, Timestamp.getDefaultInstance());
			document.append(MESSAGETYPE, 0);
			document.append(TERMID, 2);
			document.append(SENDER_ID, "shefali");
			document.append(RECEIVER_ID, "rency");
			document.append(PAYLOAD, "Hi, this is me");
			Date date1 = new Date();

			Date date = new Date(date1.toString());
			document.append(TIMESTAMP, Timestamp.getDefaultInstance());
			document.append(STATUS, 0);
			document.append(TIMESTAMPONLATESTUPDATE, Timestamp.getDefaultInstance());
			document.append(READ, 0);
			dbCollection.insertOne(document);

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public FindIterable<Document> getUnreadMessage(String key) {
		System.out.println("***MongoDB*** fn:get***");
		FindIterable<Document> result = null;
		try {
			result = dbCollection.find(Filters.and(Filters.eq(RECEIVER_ID, key), Filters.eq(READ, 0)));
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {

		}

		return result;

	}

	/*
	 * public boolean setRead(String key) { try { dbCollection.updateOne({
	 * "receiverID":key },{ $set:{ "read": 1 } }); } catch(Exception e) {
	 * e.printStackTrace();
	 * 
	 * }
	 */
}
