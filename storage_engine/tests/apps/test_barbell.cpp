#include "gtest/gtest.h"
#include "barbell.cpp"

TEST(BARBELL, HYBRID) {
  NUM_THREADS = 4;
  std::unordered_map<std::string, void *> relations;
  std::unordered_map<std::string, Trie<hybrid> *> tries;
  std::unordered_map<std::string, std::vector<void *> *> encodings;
  long result = run(relations, tries, encodings);
  EXPECT_EQ(176, result);
}