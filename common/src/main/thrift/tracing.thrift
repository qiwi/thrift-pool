/* 
		CHANGING THIS FILE REQUIRES MANUAL REGENERATION 
thrift -out common/src/main/java -I common/src/main/thrift --allow-64bit-consts --gen java:fullcamel common/src/main/thrift/tracing.thrift
'C:/Program Files/thrift/thrift' -out common/src/main/java -I common/src/main/thrift --allow-64bit-consts --gen java:fullcamel common/src/main/thrift/tracing.thrift

*/
namespace java com.qiwi.thrift.tracing.thrift

struct ClientId {
  1: string name
}

struct RequestHeader {
  1: i64 trace_id,
  2: i64 span_id,
  3: optional i64 parent_span_id,
  5: optional bool sampled // if true we should trace the request, if not set we have not decided.
  6: optional ClientId client_id
  7: optional i64 flags // contains various flags such as debug mode on/off
}

