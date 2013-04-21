var pushdb = require('../')
  , assert = require('assert')
  , cleanup = require('cleanup')
  , rimraf = require('rimraf')
  ;

var done = false

var d = cleanup(function (error) {
  rimraf.sync('./testdb')
  if (error) throw error
  done = true
})
d.enter()

var db = pushdb('./testdb')

db.store('test').on('change', function (change) {
  change.index('test-index', 'test0', change.value)
  change.index('test-index', 'test1', change.value)
})

db.store('test').put('test1', 1, function (e) {
  if (e) throw e

  db.index('test-index').all(function (e, all) {
    assert.equal(all.length, 2)
    assert.equal(all[0].key(), 'test0')
    assert.equal(all[0].value, 1)
    assert.equal(all[1].key(), 'test1')
    assert.equal(all[1].value, 1)

    d.cleanup()
  })
})

process.on('exit', function () {
  if (!done) throw new Error("Didn't finish")
})