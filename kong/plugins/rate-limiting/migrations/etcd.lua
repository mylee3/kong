return {
  {
    name = "2015-08-03-132400_init_ratelimiting",
    up = function(db)
      db:create_table("ratelimiting_metrics")
    end,
    down = function(db)
      db:drop_table("ratelimiting_metrics")
    end
  }
}
