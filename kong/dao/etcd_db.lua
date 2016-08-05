local cjson = require('cjson');
local uuid = require ('lua_uuid');
local basexx = require('basexx')
local etcd = require ('etcd.luasocket');
local timestamp = require "kong.tools.timestamp"
local Errors = require "kong.dao.errors"
local BaseDB = require "kong.dao.base_db"
local utils = require "kong.tools.utils"

local EtcdDB = BaseDB:extend()

--These are called from dao.lua.  Generates UUID and timestamp.  It requires kong/tools/timestamp.lua, so may implement later
EtcdDB.dao_insert_values = {
  id = function()
    return uuid()
  end,
  timestamp = function()
    return timestamp.get_utc()
  end
}

function EtcdDB:connect()

  local cli, err = etcd.new({
    host = self.host,
    peer = self.peer
  });
  if (cli == nil) then
    return nil, "Not connected"
  end
  return cli, nil
end


function EtcdDB:new(options)
  self.host = options.host
  self.peer = options.peer
  local cli = self:connect()
  self.keyspace = options.keyspace
  self.driver_url = options.driver_url
  self.page = nil
  EtcdDB.super.new(self, "etcd", options)
end

function EtcdDB:infos()
  return {
    desc = "keyspace",
    name = self:_get_conn_options().keyspace
  }
end

--data is encoded since the source files etcd refers to has trouble with certain characters
local function encodeTable(row)
  local table = {}
  for key, val in pairs(row) do
    if(val ~= nil) then
      local value = cjson.encode(val)
      table[key] =  basexx.to_crockford(value)
    else
      table[key] = nil
    end

  end

  return table
end

local function decodeTable(encodedRow)
  local table = {}
  for key, val in pairs(encodedRow) do
    if(val ~= nil) then
      local value =  basexx.from_crockford(val)
      table[key] = cjson.decode(value)
    else
      table[key] = nil
    end
  end
  return table
end


local function check_unique_constraints(self, table_name, constraints, values, primary_keys, update)
  local errors
  if next(constraints) == nil then
    return Errors.unique(errors)
  end
  for col, constraint in pairs(constraints.unique) do
    -- Only check constraints if value is non-null
    if values[col] ~= nil then
      local rows, error = self:find_all(constraint.table, {[col] = values[col]}, constraint.schema)
      if #rows > 0 then
        if update then
          local same_row = true
          for col, val in pairs(primary_keys) do
            if val ~= rows[1][col] then
              same_row = false
              break
            end
          end

          if not same_row then
            errors = utils.add_error(errors, col, values[col])
          end
        else
          errors = utils.add_error(errors, col, values[col])
        end

      end
    end
  end
  return Errors.unique(errors)
end

local function check_foreign_constaints(self, values, constraints)
  local errors

  if next(constraints) == nil then
    return Errors.foreign(errors)
  end

  for col, constraint in pairs(constraints.foreign) do
    -- Only check foreign keys if value is non-null, if must not be null, field should be required
    if values[col] ~= nil then
      local res, err = self:find(constraint.table, constraint.schema, {[constraint.col] = values[col]})
      if err then
        return err
      elseif res == nil then
        errors = utils.add_error(errors, col, values[col])
      end
    end
  end

  return Errors.foreign(errors)
end

--BaseDB wants this implemented, but etcd does not need this.
function EtcdDB:query(query)
end

function EtcdDB:insert(table_name, schema, model, constraints, options)

  local err = check_unique_constraints(self, table_name, constraints, model)
  if err then
    return nil, err
  end

  err = check_foreign_constaints(self, model, constraints)
  if err then
    return nil, err
  end

  local cli = self:connect()

  local primary_key;

  primary_key = model[schema.primary_key[1]]

  local encodedTable = encodeTable(model)

  local res, error
  if options and options.ttl then
    local ttl = options.ttl
    if ttl > 0 then
      ttl = ttl - 1
    end
    res, error = cli:set(self.driver_url..self.keyspace..'/tables/'..table_name..'/'..primary_key, encodedTable, ttl);
  else
    res, error = cli:set(self.driver_url..self.keyspace..'/tables/'..table_name..'/'..primary_key, encodedTable);
  end
  if not error then

    local result = cjson.decode(res.body.node.value)
    local ret = decodeTable(result)

    return ret, nil

  else
    return nil, error
  end
end

function EtcdDB:find_all(table_name, filter_keys, schema)

  local cli = self:connect()

  local res, error = cli:readdir(self.driver_url..self.keyspace..'/tables/'..table_name, true);

  if error then
    return nil, error
  end

  --Kong likely only searches for filter_keys with AND properties
  local results = {}
  --Paths allows delete() without adding _self to results
  local paths = {}
  if res.status == 200 then
    if res.body.node.nodes ~= nil then
      for _,row_string in ipairs(res.body.node.nodes) do
        local row = cjson.decode(row_string.value)
        local path = row_string.key;
        local add;

        if filter_keys == nil then
          add = true
        else

          local encodedConditions = encodeTable(filter_keys)

          for k,v in pairs(encodedConditions) do -- AND
            add = (row[k] == v)
            if not add then
              break
            end
          end
        end
        if add then
          local decodedRow = decodeTable(row)
          results[#results+1] = decodedRow
          paths[#paths+1] = path
        end
      end
    end
  elseif res.status == 404 and table_name ~= "schema_migrations" then
    return nil, table_name .." table not found"
  end

  return results, nil, paths
end

function EtcdDB:find(table_name, schema, filter_keys)
  local res, err, paths = self:find_all(table_name, filter_keys, schema)
  if not err then
    if #res>0 then
      return res[1], nil, paths[1]
    else
      return nil, nil
    end
  else
    return nil, err
  end
end

function EtcdDB:count(table_name, filter_keys, schema)
  local rows, err = self:find_all(table_name, filter_keys, schema)
  if not err then
    if #rows>0 then
      return #rows
    else
      return 0
    end
  else
    return nil, err
  end
end

local function cascade_delete(self, primary_keys, constraints)
  if constraints.cascade == nil then return end

  for f_entity, cascade in pairs(constraints.cascade) do
    local tbl = {[cascade.f_col] = primary_keys[cascade.col]}
    local rows, err = self:find_all(cascade.table, tbl, cascade.schema)
    if err then
      return nil, err
    end

    for _, row in ipairs(rows) do
      local primary_keys_to_delete = {}
      for _, primary_key in ipairs(cascade.schema.primary_key) do
        primary_keys_to_delete[primary_key] = row[primary_key]
      end

      local ok, err = self:delete(cascade.table, cascade.schema, primary_keys_to_delete)
      if not ok then
        return nil, err
      end
    end
  end
end

function EtcdDB:delete(table_name, schema, filter_keys, constraints)

  local cli = self:connect()
  local res, error, paths = self:find_all(table_name, filter_keys, schema)

  if next(res) == nil then
    return nil, nil
  end

  if not error then
    for _, v in ipairs(paths) do
      local _, _err = cli:delete(v);
      if _err then
        return nil, "Error while deleting '"..v.."' (".._err..")"
      end
    end
    if constraints ~= nil then
      cascade_delete(self, filter_keys, constraints)
    end
    return res[1], nil
  else
    return nil, error
  end
end

function EtcdDB:update(table_name, schema, constraints, filter_keys, values, nils, full, model, options)

  local err = check_unique_constraints(self, table_name, constraints, values, filter_keys, true)
  if err then
    return nil, err
  end
  err = check_foreign_constaints(self, values, constraints)
  if err then
    return nil, err
  end

  -- Etcd TTL on update is per-column and not per-row, and TTLs cannot be updated on primary keys.
  -- Not only that, but TTL on other rows can only be incremented, and not decremented. Because of all
  -- of these limitations, the only way to make this happen is to do an upsert operation.

  --for etcd, we would need to test cases for ttl.
  if options and options.ttl then
    if schema.primary_key and #schema.primary_key == 1 and filter_keys[schema.primary_key[1]] then
      local row, err = self:find(table_name, schema, filter_keys)
      if err then
        return nil, err
      elseif row then
        for k, v in pairs(row) do
          if values[k] == nil then
            model[k] = v -- Populate the model to be used later for the insert
          end
        end

        local cli = self:connect()

        local encodedTable = encodeTable(model)
        local res, error = cli:setx(self.driver_url..self.keyspace..'/tables/'..table_name..'/'..model[schema.primary_key[1]], encodedTable, options.ttl);
        if not error then
          result = cjson.decode(res.body.node.value)
          ret = decodeTable(result)

          return ret, nil
        else
          return nil, error
        end

      end
    else
      return nil, "Cannot update TTL on entities that have more than one primary_key"
    end
  end

  --Here we can update row.  If it's full, revert nil fields back to nil.
  --note that values are schema fields that aren't primary keys

  local row, err = self:find(table_name, schema, filter_keys)

  if err then
    return nil, err
  elseif row then
    for k, v in pairs(row) do
      if values[k] == nil then
        model[k] = v -- Populate the model to be used later for the insert
      end
    end

    if full then
      for col in pairs(nils) do
        model[col] = nil
      end
    end


    local cli = self:connect()

    local encodedTable = encodeTable(model)

    local res, error = cli:setx(self.driver_url..self.keyspace..'/tables/'..table_name..'/'..model[schema.primary_key[1]], encodedTable);
    if not error then
      result = cjson.decode(res.body.node.value)
      ret = decodeTable(result)
      return ret, nil
    else
      return nil, error
    end
  end
end

--Function from http://stackoverflow.com/questions/15706270/sort-a-table-in-lua
local function spairs(t, order)
    -- collect the keys
    local keys = {}
    for k in pairs(t) do keys[#keys+1] = k end

    -- if order function given, sort by it by passing the table and keys a, b,
    -- otherwise just sort the keys
    if order then
        table.sort(keys, function(a,b) return order(t, a, b) end)
    else
        table.sort(keys)
    end

    -- return the iterator function
    local i = 0
    return function()
        i = i + 1
        if keys[i] then
            return keys[i], t[keys[i]]
        end
    end
end

local function find_sorted(self, table_name, filter_keys, schema)

  local cli = self:connect()

  local res, error = cli:readdir(self.driver_url..self.keyspace..'/tables/'..table_name, true, true);

  if error then
    return nil, error
  end

  --Kong likely only searches for conditions with AND properties
  local key = schema.primary_key[1]
  local results = {}
  local keys = {}
  if res.status == 200 then
    if res.body.node.nodes ~= nil then
      for _,row_string in ipairs(res.body.node.nodes) do
        local row = cjson.decode(row_string.value)
        local path = row_string.key;
        local add;

        if filter_keys == nil then
          add = true
        else
          local encodedConditions = encodeTable(filter_keys)
          for k,v in pairs(encodedConditions) do -- AND
            add = (row[k] == v)
            if not add then
              break
            end
          end
        end
        if add then
          local decodedRow = decodeTable(row)
          results[#results+1] = decodedRow
          keys[#keys+1] = decodedRow[key]
        end
      end
    end
  end
  sortedResults = {}
  for k,v in spairs(keys, function(t,a,b) return t[b] < t[a] end) do
    sortedResults[#sortedResults+1] = results[k]
  end

  return sortedResults, nil
end

--based from postgres_db.lua's find_page.
--The main problem is it requires the results to be in order every time.  However
--find_all() is out of order because of how etcd reads directories.  So we'll have
--to call find_all for the first pass and then pass in results.
function EtcdDB:find_page(table_name, tbl, page, page_size, schema)
  local results
  if page == nil then
    page = 1
    --Only calls find_sorted() the first time
    results, err = find_sorted(self, table_name, tbl, schema)
    --Stores page as a variable
    self.page = results
    if err then
      return nil, err
    end
  else
    results = self.page
  end

  local total_count = #results

  local total_pages = math.ceil(total_count/page_size)
  local offset = page_size * (page - 1)

  --this solution is so slow.  Indexing would be helpful here.
  local max_size = math.min((page * page_size), total_count)
  local rows = {}
  for k, v in ipairs(results) do
    if k > offset then
      rows[#rows + 1] = v
      if k == max_size then
        break
      end
    end
  end
  local next_page = page + 1
  return rows, nil, (next_page <= total_pages and next_page or nil)
end

function EtcdDB:create_table(table_name)
  local cli = self:connect()
  local res, error = cli:mkdirnx(self.driver_url..self.keyspace..'/tables/'..table_name);
  if not error then
    return nil
  else
    return error
  end
end

function EtcdDB:truncate_table(table_name)
  local cli = self:connect()
  local res, error, paths = self:find_all(table_name, nil, nil)
  if error then
    return error
  else
    for _, v in ipairs(paths) do
      local _, _err = cli:delete(v);
      if _err then
        return nil, "Error while deleting '"..v.."' (".._err..")"
      end
    end
  end
  return nil
end

function EtcdDB:setTTL(table_name, ttl)
  local cli = self:connect()
  local rows, err = self:find_all(table_name, {}, nil)
  if err then
    return nil, err
  end
  for _, row in ipairs(rows) do
    local encodedTable = encodedTable(self, row)
    local _, err = cli:setx(self.driver_url..self.keyspace..'/tables/oauth2_authorization_codes/' .. row.id, encodedTable, ttl)
    if err then
      return err
    end
  end
  return nil
end

--for rate-limiting and response_ratelimiting's increment()
function EtcdDB:setRow(table_name, primary_keys, model, val)

  local cli = self:connect()
  local res, error = cli:readdir(self.driver_url..self.keyspace..'/tables/'..table_name, true);
  if error then
    return nil, error
  end

  local results = {}
  local paths = {}
  if res.status == 200 then
    if res.body.node.nodes ~= nil then
      for _,row_string in ipairs(res.body.node.nodes) do
        local row = cjson.decode(row_string.value)
        local path = row_string.key;
        local add;

        if primary_keys == nil then
          add = true
        else
          local encodedConditions = encodeTable(primary_keys)
          for k,v in pairs(encodedConditions) do -- AND
            add = (row[k] == v)
            if not add then
              break
            end
          end
        end
        if add then
          local decodedRow = decodeTable(row)
          results[#results+1] = decodedRow
          paths[#paths+1] = path
        end
      end
    end
  elseif res.status == 404 then
    return nil, "table not found"
  end

  local res, err

  local encodedTable = nil
  if #results == 0 then
    local UUID = uuid()
    if(model.value == nil) then
      model.value = val
    end
    encodedTable = encodeTable(model)
    res, err = cli:set(self.driver_url..self.keyspace..'/tables/'..table_name .. '/' .. UUID, encodedTable)
  else
    if(results[1] ~= nil) then
      model.value = results[1].value + val
    end
    encodedTable = encodeTable(model)
    res, err = cli:set(paths[1], encodedTable)
  end
  if not err then
    result = cjson.decode(res.body.node.value)
    ret = decodeTable(result)

    return ret, nil
  else
    return nil, err
  end
end

function EtcdDB:drop_table(table_name)
  local cli = self:connect()
  self:truncate_table(table_name)
  local res, error = cli:rmdir(self.driver_url..self.keyspace..'/tables/'..table_name, true);
  if not error then
    return nil
  else
    return error
  end
end

function EtcdDB:current_migrations()
  local cli = self:connect()
  local res, error = self:find_all("schema_migrations", nil, nil)
  if error then
    return nil, error
  end
  return res, nil
end

function EtcdDB:record_migration(idP, nameP)
  local row, error = self:find("schema_migrations", {id=idP}, nil)
  if error then
    return nil, error
  end
  if row == nil then
    row = {id = idP, migrations = {nameP}}
  else
    row.migrations[#row.migrations+1] = nameP
  end

  local cli = self:connect()
  local encodedTable = encodeTable(row)
  local res, err = cli:set(self.driver_url..self.keyspace..'/tables/schema_migrations/' .. idP, encodedTable)
  if err then
    return err
  end
  return nil
end

return EtcdDB
