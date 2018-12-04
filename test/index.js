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
var moment = require('moment');


var dbconfig = { _driver: new JSHsqlite(), database: ':memory:' };

function initDB(db){
  return new JSHdb(dbconfig);
};
function disposeDB(db, cb){
  db.Close(cb);
}

var tempTable = 'create temp table temp_c(c_id bigint); insert into temp_c(c_id) values (1);insert into temp_c(c_id) values (2);insert into temp_c(c_id) values (3);';

describe('Basic',function(){
  var db;
  before(function(){ db = initDB(); });
  before('Create temp table', function (done) {
    //Connect to database and get data
    db.Scalar('',tempTable,[],{},function(err,rslt){
      assert(!err,'Success');
      return done();
    });
  });
  after(function(done){ disposeDB(db, done); });

  it('Select', function (done) {
    //Connect to database and get data
    var c_id = '10';
    //dbconfig.database = './test.db';
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
  it('Select Parameter', function (done) {
    //Connect to database and get data
    var c_id = '1';
    db.Recordset('','select @c_id c_id',[JSHdb.types.BigInt],{'c_id': c_id},function(err,rslt){
      assert(!err,'Success');
      assert((rslt && rslt.length && (rslt[0].c_id==c_id)),'Parameter returned correctly');
      return done();
    });
  });
  it('Scalar', function (done) {
    //Connect to database and get data
    db.Scalar('','select count(*) from temp_c',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt==3,'Scalar correct');
      return done();
    });
  });
  it('Row', function (done) {
    //Connect to database and get data
    var C_ID = '1';
    db.Row('','select * from temp_c where c_id=@C_ID;',[JSHdb.types.BigInt],{'C_ID': C_ID},function(err,rslt){
      assert(!err,'Success');
      assert(rslt && (rslt.c_id==C_ID),'Recordset correct');
      return done();
    });
  });
  it('Recordset', function (done) {
    //Connect to database and get data
    db.Recordset('','select * from temp_c;',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt && rslt.length && (rslt.length==3) && (rslt[0].c_id==1),'Recordset correct');
      return done();
    });
  });
  it('MultiRecordset', function (done) {
    //Connect to database and get data
    db.MultiRecordset('',"PRAGMA auto_vacuum=0;select * from temp_c;select count(*) cnt from temp_c;",[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt && rslt.length && (rslt.length==2),'Multiple recordsets returned');
      assert(rslt[0] && (rslt[0].length==3) && (rslt[0][0].c_id==1),'Recordset 1 correct');
      assert(rslt[1] && (rslt[1].length==1) && (rslt[1][0].cnt==3),'Recordset 2 correct');
      return done();
    });
  });
  it('Error', function (done) {
    //Connect to database and get data
    db.Command('','select b;',[],{},function(err,rslt){
      assert(err,'Success');
      return done();
    });
  });
  it('Transact-SQL', function (done) {
    //Connect to database and get data
    db.Scalar('',"drop table if exists temp.wk;  \
                  create table temp.wk(a); \
                  insert into temp.wk(a) values(1); \
                  update temp.wk set a=a+1; \
                  update temp.wk set a=a+1; \
                  update temp.wk set a=a+1; \
                  insert into temp_c(c_id) values ((select a from temp.wk));\
                  select c_id from temp_c order by c_id desc limit 1;\
                  delete from temp_c where c_id=4;",[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt==4,'Result correct');
      return done();
    });
  });
  it('Application Error', function (done) {
    //Connect to database and get data
    db.Scalar('',"update jsharmony_meta set errcode=-3,errmsg='Application Error - Test Error';",[],{},function(err,rslt){
      assert(err,'Exception raised');
      assert(err.message=='Application Error - Test Error','Application Error raised');
      return done();
    });
  });
  it('Application Warning', function (done) {
    //Connect to database and get data
    db.Scalar('',"update jsharmony_meta set errcode=-2,errmsg='Test warning';",[],{},function(err,rslt,stats){
      assert(!err, 'Success');
      assert(stats.warnings && stats.warnings.length,'Warning generated');
      assert(stats.notices && !stats.notices.length,'No notice generated');
      assert((stats.warnings[0].message=='Test warning') && (stats.warnings[0].severity=='WARNING'),'Warning valid');
      return done();
    });
  });
  it('Application Notice', function (done) {
    //Connect to database and get data
    db.Scalar('',"update jsharmony_meta set errcode=-1,errmsg='Test notice';",[],{},function(err,rslt,stats){
      assert(!err, 'Success');
      assert(stats.notices && stats.notices.length,'Notice generated');
      assert(stats.notices && !stats.warnings.length,'No warnings generated');
      assert((stats.notices[0].message=='Test notice') && (stats.notices[0].severity=='NOTICE'),'Notice valid');
      return done();
    });
  });
  it('Context', function (done) {
    //Connect to database and get data
    db.Scalar('CONTEXT',"select context from jsharmony_meta;",[],{},function(err,rslt){
      assert(rslt && (rslt.toString().substr(0,7)=='CONTEXT'),'Context found');
      return done();
    });
  });
  it('Bad Transaction', function (done) {
    //Connect to database and get data
    db.ExecTransTasks({
      task1: function(dbtrans, callback, transtbl){
        db.Command('','insert into temp_c(c_id) values(4);',[],{},dbtrans,function(err,rslt){ callback(err, rslt); });
      },
      task2: function(dbtrans, callback, transtbl){
        db.Recordset('','select * from temp_c',[],{},dbtrans,function(err,rslt){ assert(rslt && (rslt.length==4),'Row count correct'); callback(err, rslt); });
      },
      task3: function(dbtrans, callback, transtbl){
        db.Recordset('',"update jsharmony_meta set errcode=-3,errmsg='Application Error - Test Error';",[],{},dbtrans,function(err,rslt){ callback(err, rslt); });
      },
    },function(err,rslt){
      assert(err,'Rollback generated an error');
      assert(err.message=='Application Error - Test Error','Application Error raised');
      return done();
    });
  });
  it('Transaction Rolled back', function (done) {
    //Connect to database and get data
    db.Scalar('','select count(*) from temp_c',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt==3,'Row count correct');
      return done();
    });
  });
  it('Good Transaction', function (done) {
    //Connect to database and get data
    db.ExecTransTasks({
      task1: function(dbtrans, callback, transtbl){
        db.Command('','insert into temp_c(c_id) values(4);',[],{},dbtrans,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task2: function(dbtrans, callback, transtbl){
        db.Command('','insert into temp_c(c_id) values(5);',[],{},dbtrans,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task3: function(dbtrans, callback, transtbl){
        db.Command('',"update jsharmony_meta set errcode=-2,errmsg='Test warning';",[],{},dbtrans,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task4: function(dbtrans, callback, transtbl){
        db.Command('',"update jsharmony_meta set errcode=-1,errmsg='Test notice';",[],{},dbtrans,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task5: function(dbtrans, callback, transtbl){
        db.Recordset('',"select count(*) count from temp_c",[],{},dbtrans,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    },function(err,rslt,stats){
      assert(!err,'Success');
      assert((rslt.task5.length==1)&&(rslt.task5[0].count==5),'Correct result');
      assert((stats.task3.warnings[0].message=='Test warning'),'Warning generated');
      assert((stats.task4.notices[0].message=='Test notice'),'Notice generated');
      return done();
    });
  });
  it('Transaction Committed', function (done) {
    //Connect to database and get data
    db.Scalar('','select count(*) from temp_c',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt==5,'Row count correct');
      return done();
    });
  });
  it('Drop temp table', function (done) {
    //Connect to database and get data
    db.Scalar('','drop table temp_c;',[],{},function(err,rslt){
      assert(!err,'Success');
      return done();
    });
  });
  it('ExecTasks - One item', function (done) {
    //Connect to database and get data
    db.ExecTasks([
      function(callback){
        db.Recordset('','select 1 a;',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      }
    ],function(err,rslt,stats){
      assert(!err,'Success');
      assert(rslt&&(rslt.length==1)&&(rslt[0].length==1)&&(rslt[0][0].a==1),'Correct result');
      return done();
    });
  });
  it('ExecTasks - Parallel', function (done) {
    //Connect to database and get data
    db.ExecTasks({
      task1: function(callback){
        db.Recordset('','select 1 a;',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task2: function(callback){
        db.Recordset('','select 2 b;',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task3: function(callback){
        db.Command('',"update jsharmony_meta set errcode=-2,errmsg='Test warning';",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task4: function(callback){
        db.Command('',"update jsharmony_meta set errcode=-1,errmsg='Test notice';",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    },function(err,rslt,stats){
      assert(!err,'Success');
      assert((rslt.task1.length==1)&&(rslt.task1[0].a==1),'Correct result');
      assert((stats.task3.warnings[0].message=='Test warning'),'Warning generated');
      assert((stats.task4.notices[0].message=='Test notice'),'Notice generated');
      return done();
    });
  });
  it('ExecTasks - Serial & Parallel', function (done) {
    //Connect to database and get data
    var dbtasks = [{}, {}];
    dbtasks[0] = {
      task11: function(callback){
        db.Recordset('','select 1 a;',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task12: function(callback){
        db.Recordset('','select 2 b;',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task13: function(callback){
        db.Command('',"update jsharmony_meta set errcode=-2,errmsg='Test warning';",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task14: function(callback){
        db.Command('',"update jsharmony_meta set errcode=-1,errmsg='Test notice';",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    };
    dbtasks[1] = {
      task21: function(callback,dbrslt){
        assert(dbrslt.task11 && dbrslt.task11[0] && (dbrslt.task11[0].a==1),'Series execution worked');
        db.Recordset('','select 1 a;',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task22: function(callback){
        db.Recordset('','select 2 b;',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task23: function(callback){
        db.Command('',"update jsharmony_meta set errcode=-2,errmsg='Test warning2';",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task24: function(callback){
        db.Command('',"update jsharmony_meta set errcode=-1,errmsg='Test notice2';",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    };
    db.ExecTasks(dbtasks, function(err,rslt,stats){
      assert(!err,'Success');
      assert((rslt.task11.length==1)&&(rslt.task11[0].a==1),'Correct result');
      assert((rslt.task21.length==1)&&(rslt.task21[0].a==1),'Correct result');
      assert((stats.task13.warnings[0].message=='Test warning'),'Warning generated');
      assert((stats.task14.notices[0].message=='Test notice'),'Notice generated');
      assert((stats.task23.warnings[0].message=='Test warning2'),'Warning2 generated');
      assert((stats.task24.notices[0].message=='Test notice2'),'Notice2 generated');
      return done();
    });
  });
  it('ExecTasks - Serial & Parallel Array', function (done) {
    //Connect to database and get data
    var dbtasks = [{}, {}];
    dbtasks[0] = [
      function(callback){
        db.Recordset('','select 1 a;',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Recordset('','select 2 b;',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Command('',"update jsharmony_meta set errcode=-2,errmsg='Test warning';",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Command('',"update jsharmony_meta set errcode=-1,errmsg='Test notice';",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    ];
    dbtasks[1] = [
      function(callback,dbrslt){
        assert(dbrslt[0] && dbrslt[0][0] && (dbrslt[0][0].a==1),'Series execution worked');
        db.Recordset('','select 1 a;',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Recordset('','select 2 b;',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Command('',"update jsharmony_meta set errcode=-2,errmsg='Test warning2';",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Command('',"update jsharmony_meta set errcode=-1,errmsg='Test notice2';",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    ];
    db.ExecTasks(dbtasks, function(err,rslt,stats){
      assert(!err,'Success');
      assert((rslt[0].length==1)&&(rslt[0][0].a==1),'Correct result');
      assert((rslt[4].length==1)&&(rslt[4][0].a==1),'Correct result');
      assert((stats[2].warnings[0].message=='Test warning'),'Warning generated');
      assert((stats[3].notices[0].message=='Test notice'),'Notice generated');
      assert((stats[6].warnings[0].message=='Test warning2'),'Warning2 generated');
      assert((stats[7].notices[0].message=='Test notice2'),'Notice2 generated');
      return done();
    });
  });
  it('DB Script Notices', function (done) {
    db.SQLExt.Scripts['test'] = {};
    db.SQLExt.Scripts['test']['dropfaketable'] = ["drop table if exists fakedbthatdoesnotexist"];
    db.RunScripts(db.platform, ['test','dropfaketable'],{},function(err,rslt,stats){
      assert(!err,'Success');
      return done();
    });
  });
  it('Date passthru', function (done) {
    //Connect to database and get data
    db.Scalar('',"select strftime('%m/%d/%Y',@dt)",[JSHdb.types.Date],{'dt': moment('2018-12-03').toDate()},function(err,rslt){
      assert(!err,'Success');
      assert(rslt=='12/03/2018','Date passthru');
      return done();
    });
  });
  it('DateTime passthru', function (done) {
    //Connect to database and get data
    db.Scalar('',"select strftime('%m/%d/%Y',@dt)",[JSHdb.types.DateTime(7)],{'dt': moment('2018-12-03').toDate()},function(err,rslt){
      assert(!err,'Success');
      assert(rslt=='12/03/2018','Date passthru');
      return done();
    });
  });
  it('Foreign Key Created', function(done){
    db.Command('','\
      create table parent(parent_id int primary key); \
      create table child( \
        child_id int primary key, \
        parent_id int, \
        foreign key(parent_id) references parent(parent_id) \
      ); \
      insert into parent(parent_id) values (1); \
      insert into child(parent_id,child_id) values (1,1); \
      insert into child(parent_id,child_id) values (1,2); \
      ',[],{},function(err,rslt){
      assert(!err,'Success');
      return done();
    });
  });
  it('Foreign Key Constraint - Parent', function(done){
    db.Command('','\
      delete from parent where parent_id=1; \
      ',[],{},function(err,rslt){
      assert(err && err.message && (err.message.indexOf('FOREIGN KEY constraint failed') >= 0),'Foreign Key works')
      return done();
    });
  });
  it('Foreign Key Constraint - Child', function(done){
    db.Command('','\
      update child set parent_id=2; \
      ',[],{},function(err,rslt){
      assert(err && err.message && (err.message.indexOf('FOREIGN KEY constraint failed') >= 0),'Foreign Key works')
      return done();
    });
  });
  it('Foreign Key Constraint - Success', function(done){
    db.Command('','\
      delete from child; delete from parent; \
      ',[],{},function(err,rslt){
      assert(!err,'Success');
      return done();
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