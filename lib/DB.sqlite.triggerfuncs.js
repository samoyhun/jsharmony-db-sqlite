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

var DB = require('jsharmony-db');

var triggerFuncs = {
  "set": {
    "params": ["COL","VAL"],
    "sql": [
      "update %%%TABLENAME%%% set %%%COL%%%=%%%VAL%%% where rowid = new.rowid"
    ]
  },
  "setif": {
    "params": ["COND","COL","VAL"],
    "sql": [
      "update %%%TABLENAME%%% set %%%COL%%%=%%%VAL%%% where rowid = new.rowid and (%%%COND%%%)"
    ]
  },
  "update": {
    "params": ["COL"],
    "sql": [
      "(ifnull(old.%%%COL%%%,'')<>ifnull(new.%%%COL%%%,''))"
    ]
  },
  "top1": {
    "params": ["SQL"],
    "sql": [
      "%%%SQL%%% limit 1"
    ]
  },
  "null": {
    "params": ["VAL"],
    "sql": [
      "(%%%VAL%%% is null)"
    ]
  },
  "errorif": {
    "params": ["COND","MSG"],
    "exec": [
      "MSG = MSG.trim();",
      "if(MSG && (MSG[0]=='\\'')) MSG = '\\'Application Error - '+MSG.substr(1);",
      "return 'select case when ('+COND.trim()+') then raise(FAIL,'+MSG+') end';"
    ]
  },
  "inserted": {
    "params": ["COL"],
    "sql": [
      "new.%%%COL%%%"
    ]
  },
  "insert_values": {
    "params": ["VALUES"],
    "sql": [
      "values(%%%VALUES%%%)"
    ]
  },
  "deleted": {
    "params": ["COL"],
    "sql": [
      "old.%%%COL%%%"
    ]
  },
  "get_insert_key": {
    "params": ["TBL","COL"],
    "sql": [
      "(select %%%COL%%% from %%%TBL%%% where rowid = last_insert_rowid())"
    ]
  },
  "with_insert_identity": {
    "params": ["TABLE","COL","INSERT_STATEMENT","..."],
    "exec": [
      "var rslt = INSERT_STATEMENT.trim() + '\\\\;\\n';",
      "var EXEC_STATEMENT = [].slice.call(arguments).splice(3,arguments.length-3).join(',');",
      "var INSERT_ID = '(select '+COL+' from '+TABLE+' where rowid = last_insert_rowid())';",
      "EXEC_STATEMENT = EXEC_STATEMENT.replace(/@@INSERT_ID/g,INSERT_ID);",
      "rslt += EXEC_STATEMENT;",
      "return rslt;"
    ]
  },
  "increment_changes": {
    "params": ["NUM"],
    "sql": [
      "update jsharmony_meta set extra_changes=extra_changes+ifnull(%%%NUM%%%,1)"
    ]
  },
  "return_insert_key": {
    "params": ["TBL","COL","SQLWHERE"],
    "sql": [
      "update jsharmony_meta set last_insert_rowid_override=(select %%%COL%%% from %%%TBL%%% where %%%SQLWHERE%%%)"
    ]
  },
  "clear_insert_identity": {
    "params": [],
    "sql": "update jsharmony_meta set last_insert_rowid_override=null",
  },
  "last_insert_identity": {
    "params": [],
    "sql": "(select $ifnull(last_insert_rowid_override,last_insert_rowid()) from jsharmony_meta)"
  },
  "concat":{
    "params": [],
    "exec": [
      "var args = [].slice.call(arguments);",
      "if(args.length<1) return 'null';",
      "var rslt = '(' + args[0];",
      "for(var i=1;i<args.length;i++){",
      "  rslt += ' || ' + args[i];",
      "}",
      "rslt += ')';",
      "return rslt;"
    ]
  }
};

for(var funcname in triggerFuncs){
  var func = triggerFuncs[funcname];
  if('exec' in func) func.exec = DB.util.ParseMultiLine(func.exec);
}

exports = module.exports = triggerFuncs;