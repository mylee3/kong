return {
  {
    name = "2015-09-16-132400_init_hmacauth",
    up = function(db)
      db:create_table("hmacauth_credentials")
    end,
    down = function(db)
      db:drop_table("hmacauth_credentials")
    end
  }
}
