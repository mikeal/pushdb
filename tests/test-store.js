var pushdb = require('../')
  , assert = require('assert')
  , cleanup = require('cleanup')
  , rimraf = require('rimraf')
  , done = false
  ;

var d = cleanup(function (error) {
  rimraf.sync('./testdb')
  done = true
})
d.enter()

var db = pushdb('./testdb')

db.store('test-store').put('test1', 'yes', function (e, i) {
  db.store('test-store').get('test1', function (e, i) {
    assert.equal(i, 'yes')

    // Set it again before we check the total length.
    db.store('test-store').put('test1', 'no', function (e, i) {

      db.store('test-store').all(function (e, all) {
        assert.equal(all.length, 1)
        assert.equal(all[0].value, 'no')
        d.cleanup()
      })
    })
  })
})

process.on('exit', function () {
  if (!done) throw new Error("Didn't finish")
})