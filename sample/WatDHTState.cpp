#include "WatDHTState.h"
#include <pthread.h>

namespace WatDHT {

WatDHTState::WatDHTState() : dht_state(INIT) {
  pthread_mutex_init(&wait_on_state, NULL);
  pthread_cond_init(&state_change, NULL);
}

WatDHTState::~WatDHTState() {
  pthread_mutex_destroy(&wait_on_state);
  pthread_cond_destroy(&state_change);
}

void WatDHTState::change_state(State state) {
  pthread_mutex_lock(&wait_on_state);
  dht_state = state;
  pthread_cond_broadcast(&state_change);
  pthread_mutex_unlock(&wait_on_state);
}

void WatDHTState::wait_e(State state) {
  pthread_mutex_lock(&wait_on_state);
  while (dht_state != state) {
    pthread_cond_wait(&state_change, &wait_on_state);
  } 
  pthread_mutex_unlock(&wait_on_state);
}

void WatDHTState::wait_ge(State state) {
  pthread_mutex_lock(&wait_on_state);
  while (dht_state < state) {
    pthread_cond_wait(&state_change, &wait_on_state);
  }
  pthread_mutex_unlock(&wait_on_state);
}
} // namespace WatDHT

