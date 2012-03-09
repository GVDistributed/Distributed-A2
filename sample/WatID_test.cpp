#include "WatID.h"
#include <stdio.h>

using namespace WatDHT;

int main( int argc, const char* argv[]) {
  const char* ip = "192.168.0.5";
  const char* ip1 = "192.168.0.1";
  WatID test_node;
  test_node.set_using_md5(ip);
  test_node.debug_md5();
  for (int i = 0; i < 4; ++i) {
    printf("Bucket %d bit %d: %d\n", i, 1, test_node.get_kth_bin(i, 1));
  }
  WatID test_node1;
  test_node1.set_using_md5(ip1);
  test_node1.debug_md5();
  if (test_node == test_node1) {
    printf("Same\n");
  } else {
    printf("Different\n");
  }
  printf("Highest bin that matches: %d\n", test_node.hmatch_bin(test_node1, 1));
  if (test_node < test_node1) {
    printf("Less than\n");
  }
  if (test_node <= test_node1) {
    printf("Less than or equal\n");
  }
  if (test_node > test_node1) {
    printf("Greater than\n");
  }
  if (test_node >= test_node1) {
    printf("Greater than or equal\n");
  }
  printf("Distance:\n");
  test_node.distance(test_node1).debug_md5();
  printf("Distance CR\n");
  test_node.distance_cr(test_node1).debug_md5();
  printf("Distance CCR\n");
  test_node.distance_ccr(test_node1).debug_md5();
}
