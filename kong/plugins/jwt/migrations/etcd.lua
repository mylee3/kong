return {
  {
    name = "2015-06-09-jwt-auth",
    up = function(db)
      db:create_table("jwt_secrets")
    end,
    down = function(db)
      db:drop_table("jwt_secrets")
    end
  }

}
