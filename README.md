# ChatApp275

<b> Main Features </b><br>
Implemented the server in JAVA and client in Python. Used the Java Netty Framework for the communication between server and client. Netty is an asynchronous event-driven network application framework for rapid development of maintainable high performance protocol servers & clients<br>
Implemented RAFT algorithm from scratch - We had multiple worker threads in a thread pool to manage the incoming requests,  Leader selection from the internal servers connected including re-election if a leader fails thus ensuring fault tolerance and availability.<br>
3. Used UDP discovery for discovering nearby external servers from other teams(cross connections) so as to allow message from users registered with a different server. Used Google Protobuf for server to server communication instead of JSON to avoid nesting and decreasing the amount of data to be sent over network.<br>
Mongo DB used as the backend database to store these messages.<br>
