# ===================
# jsharmony-db-sqlite
# ===================

jsHarmony Database Connector for SQLite

## Installation

npm install jsharmony-db-sqlite --save

## Usage

```javascript
var JSHsqlite = require('jsharmony-db-sqlite');
var JSHdb = require('jsharmony-db');
var dbconfig = { _driver: new JSHsqlite(), filename: ":memory:" /* or path */ };
var db = new JSHdb(dbconfig);
db.Recordset('','select * from c where c_id >= @c_id',[JSHdb.types.BigInt],{'c_id': 10},function(err,rslt){
  console.log(rslt);
  done();
});
```

This library uses NPM's sqlite3.

## Release History

* 1.0.0 Initial release