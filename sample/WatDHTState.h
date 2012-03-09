#ifndef _WAT_DHT_STATE_H_
#define _WAT_DHT_STATE_H_

#include <pthread.h>

namespace WatDHT {

class WatDHTState {
 public:
  // Add more states as needed.
  enum State {INIT, SERVER_CREATED};

  WatDHTState();
  ~WatDHTState();
  // Change the internal state of WatDHTState.
  void change_state(State state);  
  // Wait until state equals the parameter.
  void wait_e(State state);
  // Wait until state is greater than or equal to the parameter.
  void wait_ge(State state);

 private:
  State dht_state;
  pthread_cond_t state_change;
  pthread_mutex_t wait_on_state;
};
} // namespace WatDHT

#endif
