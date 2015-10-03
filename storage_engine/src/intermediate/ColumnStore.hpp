/******************************************************************************
*
* Author: Christopher R. Aberger
*
* Stores a ColumnStore in a column wise fashion. Can take in any number of 
* different template arguments. Each template arguments corresponds to the
* type of the column. 
******************************************************************************/

#ifndef _COLUMN_STORE_H_
#define _COLUMN_STORE_H_

// helpers
template <typename T>
struct id { using type = T; };

template <typename T>
using type_of = typename T::type;

template <size_t... N>
struct sizes : id <sizes <N...> > { };

// choose N-th element in list <T...>
template <size_t N, typename... T>
struct Choose;

template <size_t N, typename H, typename... T>
struct Choose <N, H, T...> : Choose <N-1, T...> { };

template <typename H, typename... T>
struct Choose <0, H, T...> : id <H> { };

template <size_t N, typename... T>
using choose = type_of <Choose <N, T...> >;

// given L>=0, generate sequence <0, ..., L-1>
template <size_t L, size_t I = 0, typename S = sizes <> >
struct Range;

template <size_t L, size_t I, size_t... N>
struct Range <L, I, sizes <N...> > : Range <L, I+1, sizes <N..., I> > { };

template <size_t L, size_t... N>
struct Range <L, L, sizes <N...> > : sizes <N...> { };

template <size_t L>
using range = type_of <Range <L> >;

// single ColumnStore element
template <size_t N, typename T>
class ColumnStoreElem
{
  std::vector<T> elem;
public:
  std::vector<T>&       get()       { return elem; }
  const std::vector<T>& get() const { return elem; }
  ColumnStoreElem(){}

};

// ColumnStore implementation
template <typename N, typename... T>
class ColumnStoreImpl;

template <size_t... N, typename... T>
class ColumnStoreImpl <sizes <N...>, T...> : ColumnStoreElem <N, T>...
{
  template <size_t M> using pick = choose <M, T...>;
  template <size_t M> using elem = ColumnStoreElem <M, pick <M> >;

public:
  template <size_t M>
  std::vector<pick <M>>& get() { return elem <M>::get(); }

  template <size_t M>
  const std::vector<pick <M>>& get() const { return elem <M>::get(); }

  template <size_t M>
  pick <M> append_from_string(const char *string_element){
    const pick<M> value = utils::from_string<pick<M>>(string_element);
    elem <M>::get().push_back(value);
    return value;
  }

};


template <typename... T>
struct ColumnStore : ColumnStoreImpl <range <sizeof...(T)>, T...>
{
  //return the number of columns (depends on the input thus a function)
  static constexpr std::size_t num_columns() { return sizeof...(T); }
  //stores the number of rows.
  size_t num_rows = 0;
};

#endif