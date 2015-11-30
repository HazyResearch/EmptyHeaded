
Trie<C, ParMemoryBuffer> Query_0::getInput(
    int base, int pow, int n
) {
  std::vector<C> annotation_b (n);
  for (uint32_t i = 0; i < annotation_b.size(); i++) {
    annotation_b[i] = cos((2.0*M_PI*i)/n) / n;
  }
  EncodedColumnStore<C> encoded_b (&annotation_b);

  std::vector<std::vector<uint32_t>> bColumns(pow);
  getIndices(base, pow, 0, std::vector<uint32_t>(pow), bColumns);

  for (int c = 0; c < pow; c++) {
    encoded_b.add_column(&bColumns[c], base);
  }

  auto start_time = timer::start_clock();
  Trie<C, ParMemoryBuffer> trie_b(
      "/Users/egan/Documents/Projects/EmptyHeaded/examples/fft/"
          "db/relations/b",
      &encoded_b.max_set_size,
      &encoded_b.data,
      &encoded_b.annotation
  );
  timer::stop_clock("BUILDING TRIE b", start_time);

  return trie_b;
};

Trie<C, ParMemoryBuffer> Query_0::getFactorTrie(
    int b, int m, int j, int k
) {
  std::vector<std::vector<uint32_t>> factorColumns(2);
  std::vector<C> annotation_factor;

  for (int col1 = 0; col1 < b; col1++) {
    for (int col2 = 0; col2 < b; col2++) {
      factorColumns[0].push_back(col1);
      factorColumns[1].push_back(col2);

      C annotValue (1.0, 0.0);
      if (col1 * col2 > 0) {
        double exponent = ((double)col1*col2)/(pow(b, m-j-k));
        double re = cos(2*M_PI*exponent);
        double im = sin(2*M_PI*exponent);
        annotValue = C(re, im);
      }
      annotation_factor.push_back(annotValue);
    }
  }

  EncodedColumnStore<C> encoded_factor (&annotation_factor);
  encoded_factor.add_column(&factorColumns[0], b);
  encoded_factor.add_column(&factorColumns[1], b);

  char fnamebuf[20];
  snprintf(fnamebuf, sizeof(fnamebuf), "factor_%d_%d", j, k);
  std::string filename = fnamebuf;
  Trie<C, ParMemoryBuffer> trie_factor(
      "/Users/egan/Documents/Projects/EmptyHeaded/examples/fft/"
          "db/relations/"+filename,
      &encoded_factor.max_set_size,
      &encoded_factor.data,
      &encoded_factor.annotation
  );
  return trie_factor;
}

/**
 * Get indices in reverse binary order
 */
void Query_0::getIndices(
    int b, int p, int level, std::vector<uint32_t> row,
    std::vector<std::vector<uint32_t>>& results) {
  if (p == level) {
    for (int col = 0; col < p; col++) {
      results[col].push_back(row[p-col-1]);
    }
    return;
  }
  for (int i = 0; i < b; i++) {
    row[level] = i;
    getIndices(b, p, level+1, row, results);
  }
}
