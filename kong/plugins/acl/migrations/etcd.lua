return {
  {
    name = "2015-08-25-841841_init_acl",
    up = function(db)
      db:create_table("acls")
    end,
    down = function(db)
      db:drop_table("acls")
    end
  }
}
