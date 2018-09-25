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

function DBmeta(db){
  this.db = db;
}

DBmeta.prototype.getTables = function(table, callback){
  var tables = [];
  var messages = [];
  var sql_param_types = [];
  var sql_params = {};
  var sql = "select '' schema_name, name table_name , name description \
    from sqlite_master \
    WHERE (type='table' or type='view') \
      ";
  if(table){
    sql += "and name=@table_name";
    sql_param_types = [dbtypes.VarChar(dbtypes.MAX)];
    sql_params = {'table_name':(table.schema?(table.schema+'_'):'')+table.name};
  }
  this.db.Recordset('',sql,sql_param_types,sql_params,function(err,rslt){
    if(err){ return callback(err); }
    for(var i=0;i<rslt.length;i++){
      var dbtable = rslt[i];
      if(!table){
        if(dbtable.table_name.substr(0,10)=='jsharmony_') continue;
        if(dbtable.table_name=='sqlite_sequence') continue;
      }
      tables.push({
        schema:dbtable.schema_name,
        name:dbtable.table_name,
        description:dbtable.description,
        model_name:(dbtable.schema_name?(dbtable.schema_name+'_'+dbtable.table_name):dbtable.table_name)
      });
    }
    return callback(null, messages, tables);
  });
}

DBmeta.prototype.getTableFields = function(tabledef, callback){
  var _this = this;
  var fields = [];
  var messages = [];
  var x = "select \
    c.column_name column_name, \
    data_type type_name, \
    character_maximum_length max_length, \
    case when c.numeric_precision is not null then c.numeric_precision when c.datetime_precision is not null then c.datetime_precision else null end \"precision\", \
    numeric_scale \"scale\", \
    case when column_default is not null or is_nullable='YES' then 0 else 1 end required, \
    case when is_updatable='NO' or (column_default is not null and column_default like 'nextval(%') then 1 else 0 end readonly, \
    pgd.description description, \
    case when ( \
      SELECT string_to_array(pg_index.indkey::text,' ')::int4[] \
        FROM pg_index \
        WHERE pg_index.indrelid = t.oid AND \
              pg_index.indisprimary = 'true' \
      ) && ARRAY[c.ordinal_position::int4] then 1 else 0 end primary_key \
    FROM pg_catalog.pg_class t \
      inner join pg_catalog.pg_namespace n on n.oid = t.relnamespace \
      inner join information_schema.columns c on (c.table_schema=n.nspname and c.table_name=t.relname) \
      left outer join pg_catalog.pg_description pgd on (pgd.objoid=t.oid and pgd.objsubid=c.ordinal_position) \
    where t.relkind in ('r','v') and n.nspname NOT IN ('pg_catalog', 'information_schema')  \
      and t.relname=@table_name and n.nspname=@schema_name \
    order by c.ordinal_position \
  ";
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

exports = module.exports = DBmeta;