

/**
 * Thrift definitions for a messaging service
 *
 */

namespace cpp polyp
namespace java polyp
namespace python polyp_gen


struct Header
{
  1: string id
  2: string sender
  3: string type
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


