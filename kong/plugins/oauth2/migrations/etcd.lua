return {
  {
    name = "2015-08-03-132400_init_oauth2",
    up = function(db)
      db:create_table("oauth2_credentials")
      db:create_table("oauth2_authorization_codes")
      db:create_table("oauth2_tokens")
    end,
    down = function(db)
      db:drop_table("oauth2_credentials")
      db:drop_table("oauth2_authorization_codes")
      db:drop_table("oauth2_tokens")
    end
  },
  {
    name = "2016-02-29-435612_remove_ttl",
    up = function(db, properties)
      local error = db:setTTL("oauth2_authorization_codes", 0)
      if error then
        return error
      end
    end,

    down = function(db, properties)
      local error = db:setTTL("oauth2_authorization_codes", 3600)
      if error then
        return error
      end
    end
  }
}
