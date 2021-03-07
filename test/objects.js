/*
Copyright 2020 apHarmony

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
var shouldBehaveLikeAnObject = require('jsharmony-db/tests-shared/objects');
var assert = require('assert');
var path = require('path');
var fs = require('fs');
var _ = require('lodash');
var os = require('os');
var moment = require('moment');

var dbconfig = { _driver: new JSHsqlite(), database: ':memory:' };

var db = new JSHdb(dbconfig);
db.platform.Config.schema_replacement = [
  { 
    "search_schema": "{schema}.",
    "replace_schema": "{schema}_"
  },
  { 
    "search_schema": "test.",
    "replace_schema": "test_"
  }
];

describe('SQLite Objects',function(){
  var rowcountSql = function(sql) {
    return sql + ';\nselect changes()+(select extra_changes from jsharmony_meta) xrowcount';
  }
  shouldBehaveLikeAnObject(db, JSHdb, "'2020-07-07'", '2020-07-07', rowcountSql);
});