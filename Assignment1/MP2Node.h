#ifndef MP2NODE_H_
#define MP2NODE_H_

#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;

	int initialized;

	vector<Node> OldhasMyReplicas;
	vector<Node> OldhaveReplicasOf;

	long transactionID;

	map<long, int> transTime; 
	map<long, string > transKey;
	map<long, string > transValue;
	map<long, string > transValue2;
	map<long, MessageType> transType;
	map<long, int> transNum;
	map<long, bool> transComplete;
	map<long, int> transInvalid;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet 
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	~MP2Node();

	void incrementTransaction();
	void clientCreateMap(string key, string value);
	void clientDeleteMap(string key);

	void createPrimaryKeyValHelper(string key);
	void createKeyValuePrimary(string key);
	void createKeyValueTertiary(string key);

	void createMessageHandler(Message messageR);
	void readMessagehandler(Message messageR);
	void updateMessagehandler(Message messageR);
	void deleteMessagehandler(Message messageR);

	void genericMessageHandler(Message msg, long transactionID, MessageType mType);
	void readreplyMessageHandler(Message messageR, long transactionID);

	void transactionsTimeout();

	void clearMap(long transactionID);

};

#endif /* MP2NODE_H_ */