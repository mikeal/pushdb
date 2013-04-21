var pushdb = require('../')
  , assert = require('assert')
  , cleanup = require('cleanup')
  , rimraf = require('rimraf')
  ;

var done = false

var d = cleanup(function (error) {
  rimraf.sync('./testdb')
  done = true
})
d.enter()

var db = pushdb('./testdb')

db.store('test').on('change', function (change) {
  if (change.value) {
    change.index('test-index', 'test0', change.value)
    change.index('test-index', 'test1', change.value)
  }
})

db.store('test').put('test1', 1, function (e) {
  if (e) throw e

  db.store('test').put('test1', 0, function (e) {
    if (e) throw e

    db.index('test-index').all(function (e, all) {
      assert.equal(all.length, 0)

      d.cleanup()
    })
  })
})

process.on('exit', function () {
  if (!done) throw new Error("Didn't finish")
})