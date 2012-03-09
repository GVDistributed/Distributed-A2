#include "WatDHTHandler.h"
#include <string>
#include <vector>
#include <map>

#include "WatDHTServer.h"

namespace WatDHT {

WatDHTHandler::WatDHTHandler(WatDHTServer* dht_server) : server(dht_server) {
  // Your initialization goes here
}

WatDHTHandler::~WatDHTHandler() {}

void WatDHTHandler::get(std::string& _return, const std::string& key) {
  // Your implementation goes here
  printf("get\n");
}    
    
void WatDHTHandler::put(const std::string& key,
                        const std::string& val, 
                        const int32_t duration) {
  // Your implementation goes here
  printf("put\n");
}

void WatDHTHandler::join(std::vector<NodeID> & _return, const NodeID& nid) {
  // Your implementation goes here
  printf("join\n");
}    
    

void WatDHTHandler::ping(std::string& _return) {
  // Your implementation goes here
  printf("ping\n");
  _return = server->get_id().to_string(); 
} 

void WatDHTHandler::maintain(std::vector<NodeID> & _return, 
                             const std::string& id, 
                             const NodeID& nid) {
  // Your implementation goes here
  printf("maintain\n");
}

void WatDHTHandler::migrate_kv(std::map<std::string, std::string> & _return, 
                               const std::string& nid) {
  // Your implementation goes here
  printf("migrate_kv\n");
}

void WatDHTHandler::gossip_neighbors(std::vector<NodeID> & _return, 
                                     const NodeID& nid, 
                                     const std::vector<NodeID> & neighbors) {
  // Your implementation goes here
  printf("gossip_neighbors\n");
}

void WatDHTHandler::closest_node_cr(NodeID& _return, const std::string& id) {
  // Your implementation goes here
  printf("closest_node_cr\n");
}

void WatDHTHandler::closest_node_ccr(NodeID& _return, const std::string& id) {
  // Your implementation goes here
  printf("closest_node_ccr\n");
}    
} // namespace WatDHT

