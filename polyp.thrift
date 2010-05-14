

/**
 * Thrift definitions for a messaging service
 *
 */

namespace cpp polyp
namespace java polyp
namespace python polyp

struct Endpoint
{
  1: required string address
  2: required i32 port
}

struct Header
{
  1: string id
  2: Endpoint sender
  3: string msg_type
  4: string verb
}

struct Message 
{
  1: Header header;
  2: binary body;
}

service Daemon
{
  void receive(1: Message message);
}


