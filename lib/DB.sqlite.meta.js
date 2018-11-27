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
var dbtypes = DB.types;
var _ = require('lodash');
var async = require('async');

function DBmeta(db){
  this.db = db;
}

DBmeta.prototype.getTables = function(table, options, callback){
  var _this = this;
  options = _.extend({ ignore_jsharmony_schema: true }, options);

  var tables = [];
  var messages = [];
  var sql_param_types = [];
  var sql_params = {};
  var sql = "select '' schema_name, name table_name , name description, (case when type='table' then 'table' else 'view' end) table_type \
    from sqlite_master \
    WHERE (type='table' or type='view') \
      ";
  if(table){
    sql += "and name=@table_name";
    sql_param_types = [dbtypes.VarChar(dbtypes.MAX)];
    sql_params = {'table_name':(table.schema?(table.schema+'_'):'')+table.name};
  }
  sql += ' order by name;';
  this.db.Recordset('',sql,sql_param_types,sql_params,function(err,rslt){
    if(err){ return callback(err); }
    for(var i=0;i<rslt.length;i++){
      var dbtable = rslt[i];
      if(!table){
        if(options.ignore_jsharmony_schema && (dbtable.table_name.substr(0,10)=='jsharmony_')) continue;
        if(dbtable.table_name=='sqlite_sequence') continue;
      }
      tables.push({
        schema:dbtable.schema_name,
        name:dbtable.table_name,
        description:dbtable.description,
        table_type:dbtable.table_type,
        model_name:(dbtable.schema_name?(dbtable.schema_name+'_'+dbtable.table_name):dbtable.table_name)
      });
    }
    return callback(null, messages, tables);
  });
}

DBmeta.prototype.getAllTableFields = function(callback){
  var _this = this;
  var rsltfields = [];
  var rsltmessages = [];
  _this.getTables(undefined, { ignore_jsharmony_schema: false }, function(err, messages, tables){
    if(err) return callback(err);
    async.eachSeries(tables, function(table, table_cb){
      _this.getTableFields({ name: table.name }, function(err, messages, fields){
        if(err) return table_cb(err);
        if(fields) for(var i=0;i<fields.length;i++) rsltfields.push(fields[i]);
        if(messages) for(var i=0;i<messages.length;i++) rsltmessages.push(messages[i]);
        return table_cb();
      });
    },function(err){
      if(err) return callback(err);
      return callback(null, rsltmessages, rsltfields);
    });
  });
}

DBmeta.prototype.getTableFields = function(tabledef, callback){
  var _this = this;
  if(!tabledef) return _this.getAllTableFields(callback);

  var fields = [];
  var messages = [];
  var table_name = (tabledef.schema?(tabledef.schema+'_'):'')+tabledef.name;
  var dbdriver = _this.db.dbconfig._driver;
  _this.db.MultiRecordset('',"PRAGMA table_info('" + dbdriver.escape(table_name) + "');select count(*) cnt from sqlite_sequence where name='" + dbdriver.escape(table_name) + "';",
      [],
      {},
      function(err,rslt){
    if(err){ return callback(err); }

    var pragmacols = rslt[0];
    var autoincrement = rslt[1][0].cnt;

    //Convert to jsHarmony Data Types / Fields
    for(var i=0;i<pragmacols.length;i++){
      var pragmacol = pragmacols[i];
      var col = {
        schema_name: '',
        table_name: table_name,
        column_name: pragmacol.name,
        type_name: pragmacol.type, //integer, real, text, blob
        max_length: -1,
        precision: -1,
        scale: -1,
        required: (!pragmacol.dflt_value && pragmacol.notnull),
        readonly: pragmacol.pk && autoincrement,
        description: pragmacol.name,
        primary_key: pragmacol.pk
      };

      var field = { name: col.column_name };

      if(col.type_name=="text"){ 
        field.type = "varchar"; 
        field.length = -1;
      }

      else if(col.type_name=="integer"){ field.type = "int"; }

      else if(col.type_name=="real"){ field.type = "real"; if(field.precision && (field.precision.toString() != '24')) field.precision = 53; }

      else if(col.type_name=="blob"){ field.type = "blob"; field.length = col.max_length; }

      else if(col.type_name=="number"){ field.type = "real"; }

      else{
        messages.push('WARNING - Skipping Column: '+(tabledef.schema?tabledef.schema+'.':'')+tabledef.name+'.'+col.column_name+': Data type '+col.type_name + ' not supported.');
        continue;
      }

      field.coldef = col;
      fields.push(field);
    }
    return callback(null, messages, fields);
  });
}

DBmeta.prototype.getAllForeignKeys = function(callback){
  var _this = this;
  var rsltfields = [];
  var rsltmessages = [];
  _this.getTables(undefined, { ignore_jsharmony_schema: false }, function(err, messages, tables){
    if(err) return callback(err);
    async.eachSeries(tables, function(table, table_cb){
      _this.getForeignKeys({ name: table.name }, function(err, messages, fields){
        if(err) return table_cb(err);
        if(fields) for(var i=0;i<fields.length;i++) rsltfields.push(fields[i]);
        if(messages) for(var i=0;i<messages.length;i++) rsltmessages.push(messages[i]);
        return table_cb();
      });
    },function(err){
      if(err) return callback(err);
      return callback(null, rsltmessages, rsltfields);
    });
  });
}

DBmeta.prototype.getForeignKeys = function(tabledef, callback){
  var _this = this;
  if(!tabledef) return _this.getAllForeignKeys(callback);

  var fields = [];
  var messages = [];
  var table_name = (tabledef.schema?(tabledef.schema+'_'):'')+tabledef.name;
  var dbdriver = _this.db.dbconfig._driver;
  _this.db.MultiRecordset('',"PRAGMA foreign_key_list('" + dbdriver.escape(table_name) + "');",
      [],
      {},
      function(err,rslt){
    if(err){ return callback(err); }

    if(!rslt || !rslt.length) return callback(null, messages, fields);

    var pragmacols = rslt[0];

    //Convert to jsHarmony Data Types / Fields
    for(var i=0;i<pragmacols.length;i++){
      var pragmacol = pragmacols[i];
      var field = {
        id: pragmacol.id,
        from: {
          schema_name: '',
          table_name: table_name,
          column_name: pragmacol.from
        },
        to: {
          schema_name: '',
          table_name: pragmacol.table,
          column_name: pragmacol.to
        }
      };
      fields.push(field);
    }
    return callback(null, messages, fields);
  });
}

exports = module.exports = DBmeta;