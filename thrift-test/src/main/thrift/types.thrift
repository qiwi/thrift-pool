namespace java com.qiwi.thrift.pool.types

enum Status {
  OK = 0,
  ERROR = 1,
  AWAIT = 2,

}


/**
* Результат обработки запроса
*/
struct MapResult {
  2: required Status status;
  4: optional map<string, i64> data;
}


