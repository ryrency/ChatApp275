package gash.router.server.resources;

import org.bson.Document;

import com.mongodb.client.FindIterable;

import gash.database.UserMongoDB;
import gash.router.container.RoutingConf;
import gash.router.server.raft.MessageBuilder;
import routing.Pipe.Route;

public class MessageRequestResource implements RouteResource {

	public MessageRequestResource() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getPath() {
		// TODO Auto-generated method stub
		return "/message_request";
	}

	@Override
	public Route process(Route msg) {
		
		String receiver = msg.getMessagesRequest().getId();
		FindIterable<Document> documents = UserMongoDB.getInstance().get(receiver);
		Route response = MessageBuilder.prepareMessageResponse(msg.getMessagesRequest().getType(), documents);
		return response;
	}

}
