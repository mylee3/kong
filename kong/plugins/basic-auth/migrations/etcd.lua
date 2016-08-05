return {
  {
    name = "2015-08-03-132400_init_basicauth",
    up = function(db, properties)
      return db:create_table("basicauth_credentials")
    end,
    down = function(db)
      return db:drop_table("basicauth_credentials")
    end
  }
}

