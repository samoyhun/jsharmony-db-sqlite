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

var moment = require('moment');

exports = module.exports = {};

exports.dtParser = function(preserve_timezone, time_only){
  return function (dt) {
    if (!dt) return dt;
    var prefix = (time_only ? '1970-01-01 ':'');

    var has_timezone = false;
    if (/Z|[+-][0-9]+(:[0-9]+)?$/.test(dt)) has_timezone = true;
    var mdt = null;
    if (has_timezone) mdt = moment.parseZone(prefix+dt);
    else mdt = moment(prefix+dt);

    if (!mdt.isValid()) return dt;

    var re_micros = /:\d\d\.\d\d\d(\d+)/.exec(prefix+dt);
    var str_micros = '';
    if(re_micros){
      str_micros = re_micros[1];
    }

    var suffix = str_micros;
    if(preserve_timezone) suffix += mdt.format("Z");
    var rslt = '';
    if(time_only) rslt = mdt.format("1970-01-01THH:mm:ss.SSS")+suffix;
    else rslt = mdt.format("YYYY-MM-DDTHH:mm:ss.SSS")+suffix;
    return rslt;
  };
};

exports.boolParser = function(val){
  if(val===null) return null;
  if(val==='') return null;
  if(val===true) return true;
  if(val===false) return false;
  var valstr = val.toString().toUpperCase();
  if((valstr==='TRUE')||(valstr==='T')||(valstr==='Y')||(valstr==='YES')||(valstr==='ON')||(valstr==='1')) return true;
  if((valstr==='FALSE')||(valstr==='F')||(valstr==='N')||(valstr==='NO')||(valstr==='OFF')||(valstr==='0')) return false;
  return null;
};