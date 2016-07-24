namespace java com.qiwi.thrift.pool.server

include "types.thrift"

exception TestException {
    10: string message;
}


service DemoServer {
  /**
  * Возвращается true, если сервер готов принимать запросы
  * Возвращается false, или бросает исключение в противном случае
  */
  bool healthCheck();
  types.MapResult request(10: string text, 20:i64 id);
  types.MapResult requestWithError(10: string text, 20:i64 id) throws (1:TestException error);
  oneway void requestFullAsync(10: i64 requestId, 20: string text, 30:i64 id);
  types.MapResult responseFullAsync(10: i64 requestId);
  types.MapResult crash(10: binary trash, 20:string text);
  types.MapResult requestWithMap(10: map<string, i64> data, 20:i64 id);

  types.Status loadTest() throws (1:TestException error);
}


service SecondServer {
  /**
  * Recommend to use
  * bool healthCheck();
  */
  void test();
  string request(10: string text, 20:i64 id);
}

