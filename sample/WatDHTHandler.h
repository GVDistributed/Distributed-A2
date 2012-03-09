#ifndef _WAT_DHT_HANDLER_H_
#define _WAT_DHT_HANDLER_H_

#include "WatDHT.h"
#include <string>
#include <vector>
#include <map>

namespace WatDHT {

class WatDHTServer; // Forward declaration.
    
class WatDHTHandler : virtual public WatDHTIf {
 public:
  explicit WatDHTHandler(WatDHTServer* dht_server);
 
  virtual ~WatDHTHandler();

  void get(std::string& _return, const std::string& key);

  void put(const std::string& key,
           const std::string& val, 
           const int32_t duration);

  void join(std::vector<NodeID> & _return, const NodeID& nid);

  void ping(std::string& _return);

  void maintain(std::vector<NodeID> & _return, 
                const std::string& id, 
                const NodeID& nid);

  void migrate_kv(std::map<std::string, std::string> & _return, 
                  const std::string& nid);

  void gossip_neighbors(std::vector<NodeID> & _return, 
                        const NodeID& nid, 
                        const std::vector<NodeID> & neighbors);

  void closest_node_cr(NodeID& _return, const std::string& id);

  void closest_node_ccr(NodeID& _return, const std::string& id); 
 
 private:
   WatDHTServer* server;
  
};
} // namespace WatDHT

#endif
