#ifndef _WAT_ID_H_
#define _WAT_ID_H_

#include <string>
#include <openssl/md5.h>

namespace WatDHT {

class WatID {
 private:
  static const int uchar_bits = 8;
  unsigned char id_array[MD5_DIGEST_LENGTH];

  explicit WatID(const unsigned char* other_id_array);

 public:
  WatID() {}

  //explicit WatID(const std::string& other_id) throw(int);

  void set_using_md5(const std::string& id);
  int copy_from(const std::string& id);
  
  // Maximum 4 byte bin size
  // bin_bit_size must be a power of 2 and less than 8
  int get_kth_bin(int bin_num, int bin_bit_size) const;

  const WatID distance_cr(const WatID& endpoint) const; 
  const WatID distance_ccr(const WatID& endpoint) const;
  const WatID distance(const WatID& endpoint) const;
 
  // Highest matching bin number.
  int hmatch_bin(const WatID& other, int bin_bit_size) const;

  bool operator==(const WatID& other) const;
  bool operator<(const WatID& other) const;
  bool operator>(const WatID& other) const;

  bool operator!=(const WatID& other) const {
    return !(*this == other);
  }

  bool operator<=(const WatID& other) const {
    return !(*this > other);
  }

  bool operator>=(const WatID& other) const {
    return !(*this < other);
  }

  const std::string to_string() const {
    return std::string(reinterpret_cast<const char*>(id_array),
                       MD5_DIGEST_LENGTH);
  }

  void debug_md5() const;
};
}  // namespace WatDHT

#endif
