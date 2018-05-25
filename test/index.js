/*
Copyright 2017 apHarmony

This file is part of jsHarmony.

jsHarmony is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

jsHarmony is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this package.  If not, see <http://www.gnu.org/licenses/>.
*/

var JSHsqlite = require('../index');
var JSHdb = require('jsharmony-db');
var assert = require('assert');


global.dbconfig = { _driver: new JSHsqlite(), database: ':memory:' };

function initDB(db){
  return new JSHdb();
};
function disposeDB(db, cb){
  db.Close(cb);
}

describe('Basic',function(){
  var db;
  before(function(){ db = initDB(); });
  after(function(done){ disposeDB(db, done); });

  it('Select', function (done) {
    //Connect to database and get data
    var c_id = '10';
    //global.dbconfig.database = './test.db';
    db.Command('',"drop table if exists c; create table c (c_id integer);insert into c(c_id) values (10)",[],{},function(err,rslt){
      assert(!err,'Table c created successfully');
      db.Recordset('','select * from c where c_id=@c_id',[JSHdb.types.BigInt],{'c_id': c_id},function(err,rslt){
        assert(!err,'Table record retrieved successfully');
        assert((rslt && rslt.length && (rslt[0].c_id==c_id)),'Database record has correct value');
        done();
      });
    });
  });

  ///////////
  //ExecTasks
  ///////////
  it('ExecTasks - Object', function(done){
    var dbtasks = {};
    dbtasks['one'] = function (callback) {
      db.Recordset('system', 'select 1 rslt', [], {}, function (err, rslt) {
        assert(!err,'Error occurred during database operation');
        assert(rslt, 'No database return value');
        callback(err,rslt);
      });
    }
    dbtasks['two'] = function (callback) {
      db.Recordset('system', 'select 2 rslttwo', [], {}, function (err, rslt) {
        assert(!err,'Error occurred during database operation');
        assert(rslt, 'No database return value');
        callback(err,rslt);
      });
    }
    dbtasks['three'] = function (callback) {
      db.Recordset('system', 'pragma no_statement;', [], {}, function (err, rslt) {
        assert(!err,'Error occurred during database operation');
        assert(!rslt, 'No database return value expected');
        callback(err,rslt);
      });
    }
    db.ExecTasks(dbtasks, function (err, dbdata) {
      assert(!err,'Error running ExecTasks');
      assert(dbdata && dbdata['one'] && dbdata['one'].length && dbdata['one'][0] && (dbdata['one'][0].rslt==1),'Invalid ExecTasks return structure');
      assert(dbdata && dbdata['two'] && dbdata['two'].length && dbdata['two'][0] && (dbdata['two'][0].rslttwo==2),'Invalid ExecTasks return structure');
      assert(dbdata && !dbdata['three'],'Invalid ExecTasks return structure');
      done();
    });
  });
  it('ExecTasks - Array', function(done){
    var dbtasks = [
      function (callback) {
        db.Recordset('system', 'select 1 rslt', [], {}, function (err, rslt) {
          assert(!err,'Error occurred during database operation');
          assert(rslt, 'No database return value');
          callback(err,rslt);
        });
      },
      function (callback) {
        db.Recordset('system', 'select 2 rslttwo', [], {}, function (err, rslt) {
          assert(!err,'Error occurred during database operation');
          assert(rslt, 'No database return value');
          callback(err,rslt);
        });
      }
    ];
    db.ExecTasks(dbtasks, function (err, dbdata) {
      assert(!err,'Error running ExecTasks');
      assert(dbdata && dbdata.length && dbdata[0] && dbdata[0].length && dbdata[0][0] && (dbdata[0][0].rslt==1),'Invalid ExecTasks return structure');
      assert(dbdata && dbdata.length && dbdata[1] && dbdata[1].length && dbdata[1][0] && (dbdata[1][0].rslttwo==2),'Invalid ExecTasks return structure');
      done();
    });
  });
  it('ExecTasks - Parallel Arrays', function(done){
    var dbtasks = [
      {
        one: function (callback) {
          db.Recordset('system', 'select 1111 rslt', [], {}, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        }
      },
      {
        two: function (callback, transtbl) {
          db.Recordset('system', 'select @one rsltone,2 rslttwo', [JSHdb.types.BigInt], { one: transtbl.one[0].rslt }, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        },
        three: function (callback) {
          db.Recordset('system', 'select 3 rsltthree', [], {}, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        }
      }
    ];
    db.ExecTasks(dbtasks, function (err, dbdata) {
      assert(!err,'Error running ExecTasks');
      assert(dbdata && dbdata['one'] && dbdata['one'].length && dbdata['one'][0] && (dbdata['one'][0].rslt==1111),'Invalid ExecTasks return structure');
      assert(dbdata && dbdata['two'] && dbdata['two'].length && dbdata['two'][0] && (dbdata['two'][0].rslttwo==2),'Invalid ExecTasks return structure');
      assert(dbdata && dbdata['two'] && dbdata['two'].length && dbdata['two'][0] && (dbdata['two'][0].rsltone==1111),'Invalid ExecTasks return structure');
      assert(dbdata && dbdata['three'] && dbdata['three'].length && dbdata['three'][0] && (dbdata['three'][0].rsltthree==3),'Invalid ExecTasks return structure');
      done();
    });
  });
  it('ExecTasks - Error', function(done){
    var runsubtask = false;
    var dbtasks = [
      {
        one: function (callback) {
          db.Recordset('system', 'select 1111 abc def', [], {}, function (err, rslt) {
            assert(err,'Error should have occurred');
            callback(err,rslt);
          });
        },
      },
      {
        two: function (callback, transtbl) {
          runsubtask = true;
          db.Recordset('system', 'select 2 rslttwo', [], { }, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        },
        three: function (callback) {
          runsubtask = true;
          db.Recordset('system', 'select 3 rsltthree', [], {}, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        }
      }
    ];
    db.ExecTasks(dbtasks, function (err, dbdata) {
      assert(err,'Error should have been generated');
      assert(!runsubtask,'Subsequent parallel tasks should not have run');
      done();
    });
  });
  it('ExecTasks - Duplicate Key', function(done){
    var dbtasks = [
      {
        one: function (callback) {
          db.Recordset('system', 'select 1111 abc', [], {}, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        },
      },
      {
        one: function (callback, transtbl) {
          db.Recordset('system', 'select 2 rslttwo', [], { }, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        },
        three: function (callback) {
          db.Recordset('system', 'select 3 rsltthree', [], {}, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        }
      }
    ];
    db.ExecTasks(dbtasks, function (err, dbdata) {
      assert(err && (err.message=='DBTasks - Key one defined multiple times'),'Duplicate key error should have been generated');
      done();
    });
  });

  ////////////////
  //ExecTransTasks
  ////////////////
  it('ExecTransTasks - Object', function(done){
    var dbtasks = {};
    dbtasks['one'] = function (dbtrans, callback, transtbl) {
      db.Recordset('system', 'select 1 rslt', [], {}, dbtrans, function (err, rslt) {
        assert(!err,'Error occurred during database operation');
        assert(rslt, 'No database return value');
        callback(err,rslt);
      });
    }
    dbtasks['two'] = function (dbtrans, callback, transtbl) {
      db.Recordset('system', 'select 2 rslttwo', [], {}, dbtrans, function (err, rslt) {
        assert(!err,'Error occurred during database operation');
        assert(rslt, 'No database return value');
        callback(err,rslt);
      });
    }
    dbtasks['three'] = function (dbtrans, callback, transtbl) {
      db.Recordset('system', 'pragma no_statement;', [], {}, dbtrans, function (err, rslt) {
        assert(!err,'Error occurred during database operation');
        assert(!rslt, 'No database return value expected');
        callback(err,rslt);
      });
    }
    db.ExecTransTasks(dbtasks, function (err, dbdata) {
      assert(!err,'Error running ExecTransTasks');
      assert(dbdata && dbdata['one'] && dbdata['one'].length && dbdata['one'][0] && (dbdata['one'][0].rslt==1),'Invalid ExecTransTasks return structure');
      assert(dbdata && dbdata['two'] && dbdata['two'].length && dbdata['two'][0] && (dbdata['two'][0].rslttwo==2),'Invalid ExecTransTasks return structure');
      assert(dbdata && !dbdata['three'],'Invalid ExecTransTasks return structure');
      done();
    });
  });
  it('ExecTransTasks - Array', function(done){
    var dbtasks = [
      function (dbtrans, callback, transtbl) {
        db.Recordset('system', 'select 1 rslt', [], {}, dbtrans, function (err, rslt) {
          assert(!err,'Error occurred during database operation');
          assert(rslt, 'No database return value');
          callback(err,rslt);
        });
      },
      function (dbtrans, callback, transtbl) {
        db.Recordset('system', 'select 2 rslttwo', [], {}, dbtrans, function (err, rslt) {
          assert(!err,'Error occurred during database operation');
          assert(rslt, 'No database return value');
          callback(err,rslt);
        });
      }
    ];
    db.ExecTransTasks(dbtasks, function (err, dbdata) {
      assert(!err,'Error running ExecTransTasks');
      assert(dbdata && dbdata.length && dbdata[0] && dbdata[0].length && dbdata[0][0] && (dbdata[0][0].rslt==1),'Invalid ExecTransTasks return structure');
      assert(dbdata && dbdata.length && dbdata[1] && dbdata[1].length && dbdata[1][0] && (dbdata[1][0].rslttwo==2),'Invalid ExecTransTasks return structure');
      done();
    });
  });
  it('ExecTransTasks - Parallel Arrays', function(done){
    var dbtasks = [
      {
        one: function (dbtrans, callback, transtbl) {
          db.Recordset('system', 'select 1111 rslt', [], {}, dbtrans, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        }
      },
      {
        two: function (dbtrans, callback, transtbl) {
          db.Recordset('system', 'select @one rsltone,2 rslttwo', [JSHdb.types.BigInt], { one: transtbl.one[0].rslt }, dbtrans, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        },
        three: function (dbtrans, callback, transtbl) {
          db.Recordset('system', 'select 3 rsltthree', [], {}, dbtrans, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        }
      }
    ];
    db.ExecTransTasks(dbtasks, function (err, dbdata) {
      assert(!err,'Error running ExecTransTasks');
      assert(dbdata && dbdata['one'] && dbdata['one'].length && dbdata['one'][0] && (dbdata['one'][0].rslt==1111),'Invalid ExecTransTasks return structure');
      assert(dbdata && dbdata['two'] && dbdata['two'].length && dbdata['two'][0] && (dbdata['two'][0].rslttwo==2),'Invalid ExecTransTasks return structure');
      assert(dbdata && dbdata['two'] && dbdata['two'].length && dbdata['two'][0] && (dbdata['two'][0].rsltone==1111),'Invalid ExecTransTasks return structure');
      assert(dbdata && dbdata['three'] && dbdata['three'].length && dbdata['three'][0] && (dbdata['three'][0].rsltthree==3),'Invalid ExecTransTasks return structure');
      done();
    });
  });
  it('ExecTransTasks - Error', function(done){
    var runsubtask = false;
    var dbtasks = [
      {
        one: function (dbtrans, callback, transtbl) {
          db.Recordset('system', 'select 1111 abc def', [], {}, dbtrans, function (err, rslt) {
            assert(err,'Error should have occurred');
            callback(err,rslt);
          });
        },
      },
      {
        two: function (dbtrans, callback, transtbl) {
          runsubtask = true;
          db.Recordset('system', 'select 2 rslttwo', [], { }, dbtrans, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        },
        three: function (dbtrans, callback, transtbl) {
          runsubtask = true;
          db.Recordset('system', 'select 3 rsltthree', [], {}, dbtrans, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        }
      }
    ];
    db.ExecTransTasks(dbtasks, function (err, dbdata) {
      assert(err,'Error should have been generated');
      assert(!runsubtask,'Subsequent parallel tasks should not have run');
      done();
    });
  });
  it('ExecTransTasks - Duplicate Key', function(done){
    var dbtasks = [
      {
        one: function (dbtrans, callback, transtbl) {
          db.Recordset('system', 'select 1111 abc', [], {}, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        },
      },
      {
        one: function (dbtrans, callback, transtbl) {
          db.Recordset('system', 'select 2 rslttwo', [], { }, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        },
        three: function (dbtrans, callback, transtbl) {
          db.Recordset('system', 'select 3 rsltthree', [], {}, function (err, rslt) {
            assert(!err,'Error occurred during database operation');
            assert(rslt, 'No database return value');
            callback(err,rslt);
          });
        }
      }
    ];
    db.ExecTransTasks(dbtasks, function (err, dbdata) {
      assert(err && (err.message=='DBTasks - Key one defined multiple times'),'Duplicate key error should have been generated');
      done();
    });
  });
  it('WaitDefer', function(done){
    this.timeout(3000);
    var startTime = Date.now();
    var endTime = Date.now();
    var closed = false;
    var f = JSHdb.util.waitDefer(function(){ 
      assert(!closed,'CLOSE called multiple times');
      closed = true; 
      endTime = Date.now(); 
    }, 1000);
    setTimeout(function(){ f(); },100);
    setTimeout(function(){ f(); },500);
    setTimeout(function(){ f(); },800);
    setTimeout(function(){
      assert((((endTime-startTime)>=1700)&&((endTime-startTime)<=1900)),'CLOSE not called within target window');
      done();
    },2000);
  });
});