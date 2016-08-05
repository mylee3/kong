return {
  {
    name = "2015-08-21_init_response-rate-limiting",
    up = function(db)
      db:create_table("response_ratelimiting_metrics")
    end,
    down = function(db)
      db:drop_table("response_ratelimiting_metrics")
    end
  }
}
