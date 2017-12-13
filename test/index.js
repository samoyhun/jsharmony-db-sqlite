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

describe('Basic',function(){
  it('Select', function (done) {
    //Connect to database and get data
    var c_id = '10';
    global.dbconfig = { _driver: new JSHsqlite(), database: ':memory:' };
    //global.dbconfig.database = './test.db';
    var db = new JSHdb();
    db.Command('',"drop table if exists c; create table c (c_id integer);insert into c(c_id) values (10)",[],{},function(err,rslt){
      assert(!err,'Table c created successfully');
      db.Recordset('','select * from c where c_id=@c_id',[JSHdb.types.BigInt],{'c_id': c_id},function(err,rslt){
        assert(!err,'Table record retrieved successfully');
        assert((rslt && rslt.length && (rslt[0].c_id==c_id)),'Database record has correct value');
        db.Close(done);
      });
    });
  });
});