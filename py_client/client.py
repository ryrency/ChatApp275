# -*- coding: utf8 -*-
import sys
import socket
from pipe_pb2 import Route
from pipe_pb2 import Message
from pipe_pb2 import NetworkDiscoveryPacket
from encoder_decoder import LengthFieldProtoEncoder, LengthFieldProtoDncoder
from network_discovery import NetworkDiscover
from multiprocessing import Process
import urllib2
import time
# import datetime


# This is currently port and IP oof Nginx server
TCP_IP = 'http://10.0.0.10'
TCP_PORT = 4467
BUFFER_SIZE = 1024



class MessageClient:
    def __init__(self, buffer_size):
        self.buffer_size = buffer_size
        self.encoder = LengthFieldProtoEncoder()
        self.decoder = LengthFieldProtoDncoder()
        self.network_discover = NetworkDiscover()
        self.s = None
        self.socket_connect = None

        #todo - should fail if the http connection fails here, then should not continue
        # socket_address = self.__get_server_socket_address()
        # parts = socket_address.split(":")
        # self.host = "192.168.0.13"
        self.host = "192.168.0.13"
        self.port = 4267
        
        try:
            self.network_discover.connectUDP()
            self.network_discover.sendNetworkDiscoveryPacket()
            self.network_discover.receiveNetworkDiscoveryPacket() #Protobuf received need to extract host and port to form TCP connection  
        except Exception as ex:
            print "Ex-udp-network-discover -" 
            import traceback
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
        # finally:
        #     self.connect()

    def __get_server_socket_address(self):
        socket_address = urllib2.urlopen(TCP_IP + ":" + str(TCP_PORT)).read()
        print socket_address
        return socket_address

    def connect(self):
        self.s = socket.socket()
        self.s.connect((self.host, self.port))
        print "client connected to server: " + self.__get_server_path()

        # listen to incoming data on a new thread
        thread = Process(target = self.listen)
        thread.start()
   
    def listen(self):
        while True:
            try:
                data = self.s.recv(self.buffer_size)
                if (len(data) == 0):
                    print "connection has been closed with: " + self.__get_server_path()
                    self.close()
                    break

                print "data length: ", len(data)
                print "data reply: ", data
            except socket.timeout:
                self.close()
                print "connection timed out with: " + self.__get_server_path()
                break
            except KeyboardInterrupt:
                self.close()
                print "connection closed with: " + self.__get_server_path()
                break
    

 
    def sendMessage(self, senderId, payload, receiverId):
        route = Route()
        route.id = 1
        route.path = route.MESSAGE

        #creating message object
        message = Message()
        message.type = message.SINGLE
        message.senderId = senderId
        message.payload = payload
        ts = time.time()
        message.receiverId = receiverId
        message.timestamp = str(ts)
        message.status = message.ACTIVE
        message.action = message.POST

        #merge message to route object
        route.message.MergeFrom(message)
        
        self.send(route)

    def createUser(self, username, password, emailId):
        
        route = Route()
        route.id = 1
        route.path = route.MESSAGE
        
        userDetail = User()
        userDetail.uname = username
        userDetail.email = emailId
        userDetail.password = password
        ts = time.time()
        userDetail.recentActiveTime = str(ts)
        userDetail.action = userDetail.REGISTER
        
        route.user.MergeFrom(userDetail)
        
        self.send(route)

#     def post_message(self, message):
#         r = Route()
#         r.id = "1"
#         r.path = "/message"
#         r.payload = message
# 
#         self.send(r)

    def send(self, route):
        message = self.encoder.encode(route)
        self.s.send(message)

    def close(self):
        self.s.close()
        self.s = None

    def __get_server_path(self):
        return self.host + ":" + str(self.port)
    


    # cli for message client #
    def __get_usage(self):
        return "message client supports following commands - \n" + \
               "ping\n" + \
               "send 'message'\n" + \
               "close\n"

    def start_cli(self):
        print self.__get_usage()

        try:
            while True:
                cmd = raw_input("Enter your command:\n")
                if not self.s:
                    print "sorry socket not connected, please try again"
                else:
                    self.sendMessage(self, "user1", "Text Message 1", "user2")
        except KeyboardInterrupt:
            sys.exit(1)

mc = MessageClient(BUFFER_SIZE)
mc.start_cli()
