var levelup = require('levelup')
  , bytewise = require('bytewise')
  , uuid = require('node-uuid')
  , util = require('util')
  , events = require('events')
  , _ = require('underscore')
  ;

/* ===Storage Schema===
This documentation describes the internal key format written to leveldb.

Key Format :: [name, storagetype, [key], uuid]

name == (Any valid bytwise types) The name of the database or index
  This is the first key so that meta info about a store is sorted near the store.
  It also means that people can use names of stores and indexes that they
  know sort together for better performance when they know they'll be querying
  them together often.

storagetype == (Integer) store=0, storeMeta=1, index=2, indexMeta=3
  This field is mostly so that people can have indexes with the same names as stores
  and it also provides a clean way to seperate the meta storage.

[key] == (Array w/ one entry) The actual storage key wrapped in an array.
  We wrap the real key in an array so that we can easily pull an entire database.
  If we didn't wrap this in an array then we wouldn't have a good way of finding
  the end of a store or index that contains object keys.

uuid == (String) Only present in indexes.
  This avoids uniqueness in keyspace that we need to get around on indexes.
  It also provides a convenience reference to the index created for each parent.
*/

function metakey (key) {
  var k = _.clone(key)
  k[1] += 1
  return k
}

function mkey (key) {
  return bytewise.encode(key)
}

function Database (filename, opts) {
  opts = opts || {}
  opts.keyEncoding = 'binary'
  opts.valueEncoding = 'json'
  this.lev = levelup(filename, opts)
  this._queue = []
  this._parents = []
  this._history = []
  this._onFlush = []
}
util.inherits(Database, events.EventEmitter)
Database.prototype._dump = function (cb) {
  var results = []
    , x = this.lev.createReadStream()
    ;
  x.on('data', function (row) {
    row.key = bytewise.decode(row.key.slice())
    results.push(row)
  })
  x.on('end', function () {
    cb(results)
  })
}

Database.prototype._changeStore = function (storeName, key, value, cb) {
  this._change(storeName, [storeName, 0, [key]], key, value, 'store', cb)
}
Database.prototype._changeIndex = function (indexName, key, value, cb) {
  this._change(indexName, [indexName, 2, [key], uuid()], key, value, 'index', cb)
}

Database.prototype._change = function (store, realkey, key, value, s, cb) {
  var self = this

  function write () {
    self._onFlush.push(cb)
    self.emit(s+'-change-propogate', store, realkey, key, value)
    self._write(realkey, value)
    self._kick(realkey)
  }

  if (!self._current) {
    self._current = realkey

    self.lev.get(metakey(realkey), function (e, history) {
      if (e) {
        if (e.name !== 'NotFoundError') return cb(e)
        history = []
      }
      self._enter(realkey)
      // Schedule this history to be removed
      history.forEach(function (hist) {
        self._delete(hist)
      })
      write()
    })
  } else {
    self._enter(realkey)
    write()
  }
}

function getOnChange (emitter, db) {
  function onChange (name, realkey, key, value, cb) {
    if (_.isEqual(db, name)) {
      var change = {key:key, value:value}
      change.index = function (indexName, indexKey, indexValue) {
        emitter.db._changeIndex(indexName, indexKey, indexValue)
      }
      change.store = function (storeName, storeKey, storeValue) {
        emitter.db._changeStore(storeName, storeKey, storeValue)
      }
      emitter.emit('change', change)
    }
  }
  return onChange
}

Database.prototype.store = function (name) {
  var store = new Store(this, name)
  this.on('store-change-propogate', getOnChange(store, name))
  return store
}
Database.prototype.index = function (name) {
  return new Index(this, name)
}
Database.prototype._delete = function (realkey) {
  this._queue.push({key:mkey(realkey), type:'del'})
}
Database.prototype._write = function (realkey, value, cb) {
  this._history.push(realkey)
  if (cb) this._onFlush.push(cb)
  this._queue.push({key:mkey(realkey), value:value, type:'put'})
}
Database.prototype._delete = function (realkey, value) {
  this._queue.push({key:mkey(realkey), value:value, type:'del'})
}
Database.prototype._enter = function (name) {
  this._parents.push(name)
}
Database.prototype._kick = function (name) {
  this._parents.pop()
  if (this._parents.length === 0) this._flush()
}
Database.prototype._flush = function () {
  var queue = this._queue
    , onFlush = this._onFlush
    , current = this._current
    , history = this._history
    ;
  this._queue = []
  this._current = null
  this._history = []
  this._onFlush = []

  queue.push({type:'put', key: metakey(current), value: history})

  this.lev.batch(queue, function (e, i) {
    onFlush.forEach(function (cb) {
      if (cb) cb(e, i)
    })
  })
}

function Store (db, name) {
  this.db = db
  this.name = name
}
util.inherits(Store, events.EventEmitter)
Store.prototype.get = function (key, cb) {
  this.db.lev.get(mkey([this.name, 0, [key]]), cb)
}
Store.prototype.put = function (key, value, cb) {
  if (this.db._current) throw new Error('Cannot PUT to database in the middle of a change event.')
  this.db._changeStore(this.name, key, value, cb)
}
Store.prototype.all = function (cb) {
  var opts =
    { start: mkey([this.name, 0, null])
    , end: mkey([this.name, 0, {}])
    }
  return new Rows(this.db, opts, cb)
}
Store.prototype.createKey = function (key) {
  return [this.name, 0, [key]]
}

function Index (db, name) {
  this.db = db
  this.name = name
}
util.inherits(Index, events.EventEmitter)
Index.prototype.put = function (key, value, cb) {
  this.db.lev.put(mkey([this.name, 2, [key], uuid()], value), cb)
}
Index.prototype.get = function (key, cb) {
  var opts =
    { start: mkey([this.name, 2, [key]])
    , end: mkey([this.name, 2, [key, null]])
    }

  return new Rows(this.db, opts, cb)
}
Index.prototype.range = function (start, end, cb) {
  var opts =
    { start: mkey([this.name, 2, [start]])
    , end: mkey([this.name, 2, [end]])
    }
  return new Rows(this.db, opts, cb)
}
Index.prototype.all = function (cb) {
  var opts =
    { start: mkey([this.name, 2, null])
    , end: mkey([this.name, 2, {}])
    }
  return new Rows(this.db, opts, cb)
}
Index.prototype.createKey = function (key) {
  return [this.name, 0, [key], uuid()]
}

function RowsMutation (rows, handler, cb) {
  var self = this
  rows.on('row', function (row) {
    handler(this, row)
  })

  rows.on('error', self.emit.bind(self, 'error'))
  rows.on('end', self.emit.bind(self, 'end'))

  if (cb) {
    var results = []
    self.on('error', cb)
    self.on('row', results.push.bind(results))
    self.on('end', function () {cb(null, results)})
  }
}
util.inherits(RowsMutation, events.EventEmitter)
RowsMutation.prototype.map = function (fn, cb) {
  function map (emitter, row) {
    emitter.emit('row', _.defaults({value:fn(row)}, row))
  }
  return new RowsMutation(this, map, cb)
}
RowsMutation.prototype.filter = function (fn, cb) {
  function filter (emitter, row) {
    if (fn(row)) emitter.emit('row', row)
  }
  return new RowsMutation(this, filter, cb)
}
RowsMutation.prototype.reduce = function (fn, default_, cb) {
  if (!cb) {
    cb = default_
    default_ = null
  }
  var rows = 0
  function reduce (emitter, row) {
    rows += 1
    emitter.value = fn(emitter.value, row)
    emitter.emit('reduce', row, emitter.value)
  }
  var x = new RowsMutation(this, reduce, cb)
  x.on('end', function () {
    x.emit('row', {rows:rows, value:x.value})
  })

  if (default_) x.value = default_
  return x
}

function createRow (result) {
  var key = result.key
    , key_
    ;
  result.key = function () {
    if (key_) return key_
    key_ = bytewise.decode(key.slice())[2][0]
    return key_
  }
  return result
}

function Rows (db, opts, cb) {
  var self = this

  var r = db.lev.createReadStream(opts)
  r.on('data', function (result) {
    var row = createRow(result)
    self.emit('row', row)
  })

  if (cb) {
    var results = []
    self.on('error', cb)
    self.on('row', results.push.bind(results))
    self.on('end', function () {cb(null, results)})
  }

  r.on('error', self.emit.bind(self, 'error'))
  r.on('end', self.emit.bind(self, 'end'))

  return r
}
util.inherits(Rows, RowsMutation)

module.exports = function (filename) {return new Database(filename)}
