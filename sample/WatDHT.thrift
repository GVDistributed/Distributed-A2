namespace cpp WatDHT

enum WatDHTErrorType {
  KEY_NOT_FOUND,              // Key not found (get)
  INCORRECT_MIGRATION_SOURCE, // Sent a migration request to the wrong node.
  OL_MIGRATION_IN_PROGRESS    // Overlapping migration (migrate_kv)
}

struct NodeID {
  1: binary id, // WatID of the node.
  2: string ip, // IP address in standard octet notation (e.g. 127.0.0.1).
  3: i32 port   // TCP port that the node is listening on.
}

exception WatDHTException {
  1: WatDHTErrorType error_code,
  2: string error_message,
  3: optional NodeID node   // Specify which node is causing the exception.
                            // For example, on an INCORRECT_MIGRATION_SOURCE,
                            // this should be set to the node's clockwise 
                            // successor which should be the only person allowed
                            // to perform a migration. This will allow the 
                            // caller to issue a migration_kv to the new node.
}

service WatDHT {
  // Lookup key and return its associated value. Throws a WatDHTException
  // if the key does not exist.
  binary get(1: binary key) throws (1: WatDHTException err),

  // Insert key/value pair into the DHT.
  void put(1: binary key, 2: binary val, 3: i32 duration),

  // Join the DHT. The parameter nid specifies the location/id of the joining
  // node. Returns a list of NodeIDs that include the closest node to nid in
  // the DHT, the neighors of the closest node, and the nodes along the path.
  list<NodeID> join(1: NodeID nid),

  // Use for liveness checking. Returns the WatID of the node.
  binary ping(),

  // Returns the closest node to id and its neighbors. The parameter nid is the
  // caller's node location/id, which can be used by the intermediate nodes to
  // update their own routing table. 
  list<NodeID> maintain(1: binary id, 2: NodeID nid),

  map<binary, binary> migrate_kv(1: binary nid) throws (1: WatDHTException err),

  list<NodeID> gossip_neighbors(1: NodeID nid, 2: list<NodeID> neighbors), 

  NodeID closest_node_cr(1: binary id),
  NodeID closest_node_ccr(1: binary id)
}

