local EtcdDB = require "kong.dao.etcd_db"
local timestamp = require "kong.tools.timestamp"

local _M = EtcdDB:extend()

_M.table = "response_ratelimiting_metrics"
_M.schema = require("kong.plugins.response-ratelimiting.schema")

function _M:increment(api_idP, identifierP, current_timestamp, valueP, nameP)

  local periods = timestamp.get_timestamps(current_timestamp)
  local options = self:_get_conn_options()

  local ok = true
  for periodP, period_dateP in pairs(periods) do

    local primary_keys = {api_id = api_idP, identifier = identifierP, period_date = period_dateP, period = nameP.."_".. periodP}
    local model = primary_keys
    local res, err = _M.super.setRow(self, _M.table, primary_keys, model, valueP)
    if err then
      ok = false
      ngx.log(ngx.ERR, "[response-ratelimiting] could not increment counter for period '"..periodP.."': "..tostring(err))
    end
  end

  return ok

end

function _M:find(api_idP, identifierP, current_timestampP, periodP, nameP)
  local periods = timestamp.get_timestamps(current_timestampP)
  primary_keys = {api_id = api_idP, identifier = identifierP, period_date = periods[periodP], period = nameP.."_".. periodP}
  local rows, err = _M.super.find_all(self, _M.table, primary_keys, _M.schema)
  if err then
    return nil, err
  elseif #rows > 0 then
    return rows[1]
  end
end

function _M:count()
  return _M.super.count(self, _M.table, nil, _M.schema)
end

return {response_ratelimiting_metrics = _M}
