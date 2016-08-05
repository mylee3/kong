return {
  {
    name = "2015-01-12-175310_skeleton",
    up = function(db, properties)

      db:create_table("schema_migrations")
    end,
    down = function(db)
      db:drop_table("schema_migrations")
    end
  },
  {
    name = "2015-01-12-175310_init_schema",
    up = function(db)
      db:create_table("consumers")
      db:create_table("apis")
      db:create_table("plugins")
    end,
    down = function(db)
      db:drop_table("consumers")
      db:drop_table("apis")
      db:drop_table("plugins")
    end
  },
  {
    name = "2015-11-23-817313_nodes",
    up = function(db)
      db:create_table("nodes")
    end,
    down = function(db)
      db:drop_table("nodes")
    end
  },
  {
    name = "2016-02-25-160900_remove_null_consumer_id",
    up = function(_, _, dao)
      local rows, err = dao.plugins:find_all {consumer_id = "00000000-0000-0000-0000-000000000000"}
      if err then
        return err
      end

      for _, row in ipairs(rows) do
        row.consumer_id = nil
        local _, err = dao.plugins:update(row, row, {full = true})
        if err then
          return err
        end
      end
    end
  },
  {
    name = "2016-02-29-121813_remove_ttls",
    up = function(_, _, dao)
      for col, _ in ipairs(dao) do
        local rows, err = dao[col]:find_all {}
        if err then
          return err
        end

        for _, row in ipairs(rows) do
          local _, err = dao[col]:update(row, row, {ttl = 0})
          if err then
            return err
          end
        end
      end
    end,
    down = function(_, _, dao)
      for col, _ in pairs(dao) do
        local rows, err = dao[col]:find_all {}
        if err then
          return err
        end

        for _, row in ipairs(rows) do
          local _, err = dao[col]:update(row, row, {ttl = 3600})
          if err then
            return err
          end
        end
      end
    end
  }
}
