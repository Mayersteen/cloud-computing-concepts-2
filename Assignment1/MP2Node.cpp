/**********************************
 * FILE NAME: MP2Node.cpp
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
	initialized = 0;
	transactionID = g_transID;
}

MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

void MP2Node::updateRing() {

	vector<Node> curMemList;
	bool changed = false;

	curMemList = getMembershipList();

	sort(curMemList.begin(), curMemList.end());

	if(initialized == 0){
		ring = curMemList;
		initialized = 1;
	}

	if (!(ht->isEmpty())) {

		if (curMemList.size() != ring.size() ) {
			
			changed = true;
		} else {

			for(int i = 0; i<ring.size(); i++){

				if(curMemList[i].getHashCode() != ring[i].getHashCode()){
					changed = true;
					break;
				}

			}
		}		

	}

	if(changed){

		ring = curMemList;
		OldhasMyReplicas = hasMyReplicas;
		OldhaveReplicasOf = haveReplicasOf;

		hasMyReplicas.clear();
		haveReplicasOf.clear();

		stabilizationProtocol();
	}

}

vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

void MP2Node::clientCreate(string key, string value) {

	vector<Node> vec = findNodes(key);

	int index = 0;
	
	for (int iter = PRIMARY; iter <= TERTIARY; iter++) {
		ReplicaType type = static_cast<ReplicaType>(iter);
		Message msg(transactionID, memberNode->addr, CREATE, key, value, type);
		string toSend = msg.toString();
		emulNet->ENsend(&memberNode->addr, vec[index++].getAddress(), toSend);
	}

	clientCreateMap(key, value);
}

void MP2Node::clientCreateMap(string key, string value){

	transComplete.emplace(transactionID, false);
	transKey.emplace(transactionID, key);
	transValue.emplace(transactionID, value);
	transTime.emplace(transactionID, par->getcurrtime());
	transType.emplace(transactionID, CREATE);
	transNum.emplace(transactionID, 0);
	
	incrementTransaction();
}

void MP2Node::clientRead(string key){

	 vector<Node> vec = findNodes(key);

	 for(int i = 0; i<vec.size(); i++){
	 	Message message(transactionID, memberNode->addr, READ, key);
 		string toSend = message.toString(); 
 		emulNet->ENsend(&memberNode->addr, vec[i].getAddress(), toSend);

	 }

	transComplete.emplace(transactionID, false);
	transKey.emplace(transactionID, key);
	transTime.emplace(transactionID, par->getcurrtime());
	transType.emplace(transactionID, READ);
	transNum.emplace(transactionID, 0);
	incrementTransaction();
}

void MP2Node::clientUpdate(string key, string value){

	vector<Node> vec = findNodes(key);

	if(vec.size() == 0){
	 	log->logUpdateFail(&memberNode->addr, true, transactionID, key, value);
	 	return;
	}

	for(int i = 0; i<vec.size(); i++) {
	 	Message message(transactionID, memberNode->addr, UPDATE, key, value);
 		string toSend = message.toString(); 
 		emulNet->ENsend(&memberNode->addr, vec[i].getAddress(), toSend);

	}

	transComplete.emplace(transactionID, false);
	transKey.emplace(transactionID, key);
	transValue.emplace(transactionID, value);
	transTime.emplace(transactionID, par->getcurrtime());
	transType.emplace(transactionID, UPDATE);
	transNum.emplace(transactionID, 0);
	incrementTransaction();
}


void MP2Node::clientDelete(string key){

	vector<Node> vec = findNodes(key);

	for(int i = 0; i<vec.size(); i++){
	 	Message message(transactionID, memberNode->addr, DELETE, key);
 		string toSend = message.toString(); 
 		emulNet->ENsend(&memberNode->addr, vec[i].getAddress(), toSend);
	 }

	 clientDeleteMap(key);
}

void MP2Node::clientDeleteMap(string key){
	transComplete.emplace(transactionID, false);
	transKey.emplace(transactionID, key);
	transTime.emplace(transactionID, par->getcurrtime());
	transType.emplace(transactionID, DELETE);
	transNum.emplace(transactionID, 0);
	incrementTransaction();
}

bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {

	string read = ht->read(key);

	if(read == ""){

	 	bool retVal = ht->create(key, value);

		switch(replica) {
			case PRIMARY:
			   createKeyValuePrimary(key);
			case TERTIARY:
			   createKeyValueTertiary(key);
            default:
               break;
		}

	 	return retVal;

	} else {
	 	return false;
	}
	 
}

void MP2Node::createKeyValuePrimary(string key){

	vector<Node> vec = findNodes(key);

	 		bool insert = false;

	 		for(int i=0; i<hasMyReplicas.size(); i++){
	 			if(hasMyReplicas[i].getHashCode() != vec[1].getHashCode()){
	 				insert = true;
	 			}
	 		}

	 		if(insert){
		 		hasMyReplicas.emplace_back(vec[1]);
	 		}

	 		insert = false;

	 		for(int i=0; i<hasMyReplicas.size(); i++){
	 			if(hasMyReplicas[i].getHashCode() != vec[2].getHashCode()){
	 				insert = true;
	 			}
	 		}

	 		if(insert){
	 			hasMyReplicas.emplace_back(vec[2]);
	 		}

}

void MP2Node::createKeyValueTertiary(string key){

	 		vector<Node> vec = findNodes(key);

	 		bool insert = false;

	 		for(int i=0; i<haveReplicasOf.size(); i++){
	 			if(haveReplicasOf[i].getHashCode() != vec[0].getHashCode()){
	 				insert = true;
	 			}
	 		}

	 		if(insert){
		 		haveReplicasOf.emplace_back(vec[1]);
	 		}

	 		insert = false;
	 		for(int i=0; i<haveReplicasOf.size(); i++){
	 			if(haveReplicasOf[i].getHashCode() != vec[1].getHashCode()){
	 				insert = true;
	 			}
	 		}

	 		if(insert){
	 			haveReplicasOf.emplace_back(vec[2]);
	 		}

}

string MP2Node::readKey(string key) {
	 return ht->read(key);
}

bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {

	string read = ht->read(key);

	if( read == ""){
	   return false;
	}
	
    return ht->update(key, value);
}

bool MP2Node::deletekey(string key) {
	 	return ht->deleteKey(key);
}

void MP2Node::checkMessages() {

	char * data;
	int size;

	while ( !memberNode->mp2q.empty() ) {

		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		Message messageR(message);

		MessageType type = messageR.type;
		Address fromAddr =  messageR.fromAddr;
		long transactionID = messageR.transID;

		bool isFinished = transComplete.find(transactionID)->second;

		if(isFinished){
		 	continue;
		}

		if(type == CREATE){
		 	createMessageHandler(messageR);
		 } else if(type == READ){
			readMessagehandler(messageR);
		 } else if(type == UPDATE){
		 	updateMessagehandler(messageR);
		 } else if(type == DELETE){
		 	deleteMessagehandler(messageR);
		 } else if(type == REPLY){

		 	long transactionID = messageR.transID;

		 	MessageType Replytype = transType.find(transactionID)->second;
            
            if(Replytype == CREATE){
				genericMessageHandler(messageR, transactionID, CREATE);
		 	} else if(Replytype == UPDATE){
				genericMessageHandler(messageR, transactionID, UPDATE);
		 	} else if(Replytype == DELETE){
				genericMessageHandler(messageR, transactionID, DELETE);
		 	}

		 } else if(type == READREPLY){
		 	readreplyMessageHandler(messageR, transactionID);
		 }

	}

	transactionsTimeout();

}

void MP2Node::clearMap(long transactionID){
	transComplete.emplace(transactionID, true);
	transKey.erase(transactionID);
	transType.erase(transactionID);
	transValue.erase(transactionID);
	transValue2.erase(transactionID);
	transNum.erase(transactionID);
}

void MP2Node::transactionsTimeout(){

	map<long, int>::iterator it = transTime.begin();

	while(it!= transTime.end()){

		if((par->getcurrtime() - it->second) > 10){

			long transactionID = it->first;

			MessageType type = transType.find(transactionID)->second;

			string key = transKey.find(transactionID)->second;
			string value = transValue.find(transactionID)->second;
			
			if(type == CREATE){
				log->logCreateFail(&memberNode->addr, true, transactionID, key, value);
			} else if(type == READ) {
				log->logReadFail(&memberNode->addr, true, transactionID, key);
			} else if(type == DELETE) {
				log->logDeleteFail(&memberNode->addr, true, transactionID, key);
			} else if(type == UPDATE) {
				log->logUpdateFail(&memberNode->addr, true, transactionID, key, value);
			}

			clearMap(transactionID);
			
			transTime.erase(it++);

		} else {
			++it;
		}
	}
}

void MP2Node::readreplyMessageHandler(Message messageR, long transactionID){

 	int totalSoFar = transNum.find(transactionID)->second;

 	if(totalSoFar == 0){
 		transNum.erase(transactionID);
 		transNum.emplace(transactionID, totalSoFar+1);

 		string value = messageR.value;
 		transValue.emplace(transactionID, value);
 	} else if(totalSoFar == 1){

 		string oldVal = transValue.find(transactionID)->second;
 		string newVal = messageR.value;
 		string key = transKey.find(transactionID)->second;

 		if(oldVal == newVal && oldVal != "_"){
 			log->logReadSuccess(&memberNode->addr, true, transactionID, key, newVal);
 			clearMap(transactionID);
 			transTime.erase(transactionID);
 		} else {
 			transValue2.emplace(transactionID, newVal);
 			transNum.erase(transactionID);
 			transNum.emplace(transactionID, totalSoFar+1);
 		}

 	} else if(totalSoFar == 2) {

 		string oldVal = transValue.find(transactionID)->second;
 		string newVal = transValue2.find(transactionID)->second;

 		string myVal = messageR.value;
 		string key = transKey.find(transactionID)->second;

 		if(myVal == oldVal && oldVal != "_"){
			log->logReadSuccess(&memberNode->addr, true, transactionID, key, oldVal);
 			clearMap(transactionID);
 			transNum.erase(transactionID);
 		} else if (myVal == newVal && newVal != "_") {
 			log->logReadSuccess(&memberNode->addr, true, transactionID, key, newVal);
 			clearMap(transactionID);
 			transNum.erase(transactionID);
 		} else {
 			log->logReadFail(&memberNode->addr, true, transactionID, key);
 			clearMap(transactionID);
 			transNum.erase(transactionID);
 		}
 	}
}

void MP2Node::genericMessageHandler(Message msg, long transactionID, MessageType mType) {
	
	string key = transKey.find(transactionID)->second;
	string value = transValue.find(transactionID)->second;
	bool retVal = msg.success;
	
	if(retVal){
		int totalSoFar = transNum.find(transactionID)->second;

		if(totalSoFar>=1) {
			
			switch (mType) {
				case CREATE:
				   log->logCreateSuccess(&memberNode->addr, true, transactionID, key, value);
				   break;
				case UPDATE:
				   log->logUpdateSuccess(&memberNode->addr, true, transactionID, key, value);
				   break;
				case DELETE:
				   log->logDeleteSuccess(&memberNode->addr, true, transactionID, key);
				   break;
			}

			clearMap(transactionID);
			transTime.erase(transactionID);
		} else {
			transNum.erase(transactionID);
			transNum.emplace(transactionID, totalSoFar+1);
		}
	} else {
		int totalFail = transInvalid.find(transactionID)->second;

		if(totalFail>=1) {
			
			switch (mType) {
				case CREATE:
				   log->logCreateFail(&memberNode->addr, true, transactionID, key, value);
				   break;
				case UPDATE:
				   log->logUpdateFail(&memberNode->addr, true, transactionID, key, value);
				   break;
				case DELETE:
				   log->logDeleteFail(&memberNode->addr, true, transactionID, key);
				   break;
			}

			clearMap(transactionID);
			transTime.erase(transactionID);
		} else {
			transInvalid.erase(transactionID);
			transInvalid.emplace(transactionID, totalFail+1);
		}
	}
	
}

void MP2Node::updateMessagehandler(Message messageR){

	Address fromAddr =  messageR.fromAddr;
	long transactionID = messageR.transID;

 	string key = messageR.key;
 	string value = messageR.value;
 	ReplicaType replica = messageR.replica;

 	bool retVal = updateKeyValue(key, value, replica);

 	Message messageS(transactionID, memberNode->addr, REPLY, retVal);
	string toSendS = messageS.toString(); 
	emulNet->ENsend(&memberNode->addr, &fromAddr, toSendS);

	if(retVal) {
		log->logUpdateSuccess(&memberNode->addr, false, transactionID, key, value);
 	} else {
 		log->logUpdateFail(&memberNode->addr, false, transactionID, key, value);
 	}

}

void MP2Node::deleteMessagehandler(Message messageR){

	Address fromAddr =  messageR.fromAddr;
	long transactionID = messageR.transID;

	string key = messageR.key;

 	bool retVal = deletekey(key);

 	Message messageS(transactionID, memberNode->addr, REPLY, retVal);
	string toSendS = messageS.toString(); 
	emulNet->ENsend(&memberNode->addr, &fromAddr, toSendS);

	if(retVal){
 		log->logDeleteSuccess(&memberNode->addr, false, transactionID, key);
 	} else {
 		log->logDeleteFail(&memberNode->addr, false, transactionID, key);
 	}

}

void MP2Node::readMessagehandler(Message messageR){

    Address fromAddr =  messageR.fromAddr;
	long transactionID = messageR.transID;

	string key = messageR.key;
 	string read = readKey(key);

 	if(read != ""){
 		Message messageS(transactionID, memberNode->addr, read);
 		string toSendS = messageS.toString(); 
 		emulNet->ENsend(&memberNode->addr, &fromAddr, toSendS);
 		log->logReadSuccess(&memberNode->addr, false, transactionID, key, read);
 	} else {
 		Message messageS(transactionID, memberNode->addr, "_");
 		string toSendS = messageS.toString(); 
 		emulNet->ENsend(&memberNode->addr, &fromAddr, toSendS);
 		log->logReadFail(&memberNode->addr, false, transactionID, key);
 	}

}

void MP2Node::createMessageHandler(Message messageR){

	Address fromAddr =  messageR.fromAddr;
	long transactionID = messageR.transID;

	string key = messageR.key;
 	string value = messageR.value;
	ReplicaType replica = messageR.replica;

 	bool retVal = createKeyValue(key, value, replica);

	Message messageS(transactionID, memberNode->addr, REPLY, retVal);
	string toSendS = messageS.toString(); 
	emulNet->ENsend(&memberNode->addr, &fromAddr, toSendS);

 	if(retVal){
 		log->logCreateSuccess(&memberNode->addr, false, transactionID, key, value);
 	} else {
 		log->logCreateFail(&memberNode->addr, false, transactionID, key, value);
 	}

}

vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {

		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));

		} else {

			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}

	return addr_vec;
}

bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    } else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

void MP2Node::stabilizationProtocol() {


	 map<string, string>::iterator it;

	 for(it = ht->hashTable.begin(); it != ht->hashTable.end(); it++){

	 	string key = it->first;
	 	string value = it->second;

	 	if(OldhaveReplicasOf.size()>0){
	 		Message message(transactionID, memberNode->addr, DELETE, it->first);
	 		string toSend = message.toString(); 
	 		emulNet->ENsend(&memberNode->addr, OldhaveReplicasOf[0].getAddress(), toSend);
	 		clientDeleteMap(key);
	 	}

	 	if(OldhaveReplicasOf.size()>1){
	 		Message message(transactionID, memberNode->addr, DELETE, it->first);
	 		string toSend = message.toString(); 
	 		emulNet->ENsend(&memberNode->addr, OldhaveReplicasOf[1].getAddress(), toSend);
			clientDeleteMap(key);
	 	}

	 	if(OldhasMyReplicas.size()>0){
	 		Message message(transactionID, memberNode->addr, DELETE, it->first);
	 		string toSend = message.toString(); 
	 		emulNet->ENsend(&memberNode->addr, OldhaveReplicasOf[0].getAddress(), toSend);
			clientDeleteMap(key);
	 	}

	 	if(OldhasMyReplicas.size()>1){
	 		Message message(transactionID, memberNode->addr, DELETE, it->first);
	 		string toSend = message.toString(); 
	 		emulNet->ENsend(&memberNode->addr, OldhaveReplicasOf[1].getAddress(), toSend);
			clientDeleteMap(key);
	 	}

		vector<Node> vec = findNodes(key);

		int index = 0;
		
        for (int iter = PRIMARY; iter <= TERTIARY; iter++) {
		   ReplicaType type = static_cast<ReplicaType>(iter);
		   Message msg(transactionID, memberNode->addr, CREATE, key, value, type);
		   string toSend = msg.toString();
		   emulNet->ENsend(&memberNode->addr, vec[index++].getAddress(), toSend);
        }

		clientCreateMap(key, value);
    }

	ht->clear();

}

void MP2Node::incrementTransaction(){
	g_transID++;
	transactionID = g_transID;
}
//EOF