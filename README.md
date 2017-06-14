zkstream
========

A minimal streams-based node client for the ZooKeeper protocol.

API
---

## Client

### `mod_zkstream.connect(options[, cb])`

Opens a new ZooKeeper connection and returns the Client instance.

Parameters:
 - `options`: an Object, with keys:
   - `host`: a String, hostname or IP to connect to
   - `port`: an optional Number

### `Client#close([cb])`

Closes the connection.

Parameters:
 - `cb`: an optional Function, called once the connection has ended

### `Client#ping([cb])`

Manually pings the ZooKeeper server. Automatic pings occur every 2 seconds
without calling this function.

Parameters:
 - `cb`: an optional Function `(err)`

### `Client#list(path[, cb])`

Lists the children at a given path.

Parameters
 - `path`: a String, path to list children of
 - `cb`: an optional Function `(err, children)` with parameters:
   - `children`: an Array of Strings

Returns a request emitter (see below).

### `Client#stat(path[, cb])`

Gathers basic information about the node at a given path, including its version
number (useful if you want to delete it).

Parameters
 - `path`: a String, path to the node
 - `cb`: an optional Function `(err, stat)` with parameters:
   - `stat`: an Object, with fields:
     - `version`: a Number, latest version
     - `dataLength`: a Number, size of data in bytes stored at node
     - `numChildren`: a Number, number of children
     - `ctime`: a `mod_jsbn.BigNumber`
     - `mtime`: a `mod_jsbn.BigNumber`
     - `ephemeralOwner`: a Buffer, ID of connection that owns this node
                         if it is ephemeral

Returns a request emitter (see below).

### `Client#get(path[, cb])`

Gets the data stored in a given node.

Parameters
 - `path`: a String, path to the node
 - `cb`: an optional Function `(err, data, stat)` with parameters:
   - `data`: a Buffer
   - `stat`: an Object, same format as `stat` in `Client#stat`

Returns a request emitter (see below).

### `Client#delete(path, version[, cb])`

Deletes a given node as long as its latest version matches the given version.

Parameters
 - `path`: a String, path to the node
 - `version`: a Number, latest version to check against
 - `cb`: an optional Function `(err)`

Returns a request emitter (see below).

### `Client#create(path, data[, options[, cb]])`

Creates a new node at the given path, containing some provided data.

Parameters
 - `path`: a String, path to the node to be created
 - `data`: a Buffer
 - `options`: an optional Object, with keys:
   - `flags`: an optional Array of Strings, can be `'ephemeral'` or
              `'sequential'`
   - `acl`: an optional Array of ACL objects
 - `cb`: an optional Function `(err)`

Returns a request emitter (see below).

### `Client#set(path, data, version[, cb])`

Sets the contents of a given node as long as its latest version matches the
given version.

Parameters
 - `path`: a String, path to the node
 - `data`: a Buffer, the new data to place in the node
 - `version`: a Number, version number as returned from `Client#stat`
 - `cb`: an optional Function `(err)`

Returns a request emitter (see below).

### `Client#sync(path[, cb])`

Forces the ZK leader to sync up with its followers on the state of the given
node.

Parameters
 - `path`: a String, path to the node
 - `cb`: an optional Function `(err)`

Returns a request emitter (see below).

### `Client#watcher(path)`

Returns a watcher EventEmitter for a given path.

Parameters
 - `path`: a String, path to the node to watch

## Watchers

### `Watcher#on('created', cb)`

Registers a callback to be called when a particular node is created. If the
node already exists right now, the callback will be called straight away.

Parameters
 - `cb`: a Function `(stat)` with arguments:
   - `stat`: an Object, same as the `stat` returned by `Client#stat` above

### `Watcher#on('deleted', cb)`

Registers a callback to be called when a particular node is deleted. If the
node does not exist right now, the callback will be called straight away.

NOTE: Deleted events may be missed during network outages.

Parameters
 - `cb`: a Function `()`

### `Watcher#on('dataChanged', cb)`

Registers a callback to be called when a particular node's data has changed. Always fires immediately with the current contents of the node if it exists.

Parameters
 - `cb`: a Function `(data)` with arguments:
   - `data`: a Buffer, contents of the node

### `Watcher#on('childrenChanged', cb)`

Registers a callback to be called when a particular node's children have
changed. Always fires immediately with the current children of the node if it
exists.

Parameters
 - `cb`: a Function `(children)` with arguments:
   - `children`: an Array of Strings

## Request emitters

An interface returned by most functions on Client. A subclass of EventEmitter,
emits the following events:

 - `'reply'` `(pkt)`: emitted when a reply to this request is received.
                      Argument is the decoded ZooKeeper protocol packet.
 - `'error'` `(err)`: emitted when the request fails

## ACL objects

Some methods return or take ACL objects as a parameter. These are plain
Objects, with keys:

 - `id`: an Object, with keys:
   - `scheme`: a String, the URI scheme of the target of the ACL entry
   - `id`: a String
 - `perms`: an Array of Strings, can be `'read'`, `'write'`, `'create'`,
            `'delete'`, `'admin'`

