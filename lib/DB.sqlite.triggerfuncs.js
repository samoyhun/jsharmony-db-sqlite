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

exports = module.exports = {
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
    "sql": [
      "select case when (%%%COND%%%) then raise(FAIL,%%%MSG%%%) end\\;"
    ]
  },
  "deleted": {
    "params": ["COL"],
    "sql": [
      "old.%%%COL%%%"
    ]
  }
};