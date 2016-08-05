return {
  {
    name = "2015-07-31-172400_init_keyauth",
    up = function(db, properties)

      db:create_table("keyauth_credentials")
    end,
    down = function(db)
      db:drop_table("keyauth_credentials")
    end
  }
}
