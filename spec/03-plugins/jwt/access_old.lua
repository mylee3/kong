local helpers = require "spec.helpers"
local cjson = require "cjson"
local jwt_encoder = require "kong.plugins.jwt.jwt_parser"
local fixtures = require "spec.plugins.jwt.fixtures"
local base64 = require "base64"

local STUB_GET_URL = spec_helper.STUB_GET_URL

local PAYLOAD = {
  iss = nil,
  nbf = os.time(),
  iat = os.time(),
  exp = os.time() + 3600
}

describe("JWT access", function()
  local jwt_secret, base64_jwt_secret, rsa_jwt_secret_1, rsa_jwt_secret_2
  local client
  local api1, api2, api3, api4, api5, api6, api7, api8
  local consumer1, consumer2, consumer3
  
  setup(function()
    helpers.dao:truncate_tables()
    assert(helpers.prepare_prefix())
    
    api1 = assert(helpers.dao.apis:insert {name = "tests-jwt1", request_host = "jwt.com", upstream_url = "http://mockbin.com"})
    api2 = assert(helpers.dao.apis:insert {name = "tests-jwt2", request_host = "jwt2.com", upstream_url = "http://mockbin.com"})
    api3 = assert(helpers.dao.apis:insert {name = "tests-jwt3", request_host = "jwt3.com", upstream_url = "http://mockbin.com"})
    api4 = assert(helpers.dao.apis:insert {name = "tests-jwt4", request_host = "jwt4.com", upstream_url = "http://mockbin.com"})
    api5 = assert(helpers.dao.apis:insert {name = "tests-jwt5", request_host = "jwt5.com", upstream_url = "http://mockbin.com"})
    
    consumer1 = assert(helpers.dao.consumers:insert {username = "jwt_tests_consumer"})
    consumer2 = assert(helpers.dao.consumers:insert {username = "jwt_tests_base64_consumer"})
    consumer3 = assert(helpers.dao.consumers:insert {username = "jwt_tests_rsa_consumer_1"})
    consumer3 = assert(helpers.dao.consumers:insert {username = "jwt_tests_rsa_consumer_2"})
    
    assert(helpers.dao.plugins:insert {name = "jwt", config = {}, api_id = api1})
    assert(helpers.dao.plugins:insert {name = "jwt", config = {uri_param_names = {"token", "jwt"}}, api_id = api2})
    assert(helpers.dao.plugins:insert {name = "jwt", config = {claims_to_verify = {"nbf", "exp"}}, api_id = api3})
    assert(helpers.dao.plugins:insert {name = "jwt", config = {key_claim_name = "aud"}, api_id = api4})
    assert(helpers.dao.plugins:insert {name = "jwt", config = {secret_is_base64 = true}, api_id = api5})
    
    jwt_secret = assert(helpers.dao.jwt_secrets:insert {consumer_id = consumer1.id})
    base64_jwt_secret = assert(helpers.dao.jwt_secrets:insert {consumer_id = consumer2.id})
    rsa_jwt_secret_1 = assert(helpers.dao.jwt_secrets:insert {consumer_id = consumer3.id, algorithm = "RS256", rsa_public_key = fixtures.rs256_public_key})
    rsa_jwt_secret_2 = assert(helpers.dao.jwt_secrets:insert {consumer_id = consumer4.id, algorithm = "RS256", rsa_public_key = fixtures.rs256_public_key})
    
    assert(helpers.start_kong())
    client = assert(helpers.http_client("127.0.0.1", helpers.test_conf.proxy_port))
  end)

  teardown(function()
    spec_helper.stop_kong()
  end)

  describe("refusals", function()
    it("should return 401 Unauthorized if no JWT is found in the request", function()
      local res = assert(client:send {
        method = "GET",
        path = "/request",
        headers = {
          ["Host"] = "jwt.com"
        }
      })
      assert.res_status(401, res)
    end)
    it("should return return 401 Unauthorized if the claims do not contain the key to identify a secret", function()
      local jwt = jwt_encoder.encode(PAYLOAD, "foo")
      local authorization = "Bearer "..jwt
      local res = assert(client:send {
        method = "GET",
        path = "/request",
        headers = {
          ["Authorization"] = authorization,
          ["Host"] = "jwt.com"
        }
      })
      assert.res_status(401, res)
      assert.equal("No mandatory 'iss' in claims", res.message)
    end)
    it("should return 403 Forbidden if the iss does not match a credential", function()
      PAYLOAD.iss = "123456789"
      local jwt = jwt_encoder.encode(PAYLOAD, jwt_secret.secret)
      local authorization = "Bearer "..jwt
      local response, status = http_client.get(STUB_GET_URL, nil, {host = "jwt.com", authorization = authorization})
      assert.equal(403, status)
      local body = json.decode(response)
      assert.equal("No credentials found for given 'iss'", body.message)
    end)
    it("should return 403 Forbidden if the signature is invalid", function()
      PAYLOAD.iss = jwt_secret.key
      local jwt = jwt_encoder.encode(PAYLOAD, "foo")
      local authorization = "Bearer "..jwt
      local response, status = http_client.get(STUB_GET_URL, nil, {host = "jwt.com", authorization = authorization})
      assert.equal(403, status)
      local body = json.decode(response)
      assert.equal("Invalid signature", body.message)
    end)
    it("should return 403 Forbidden if the alg does not match the credential", function()
      local header = {typ = "JWT", alg = 'RS256'}
      local jwt = jwt_encoder.encode(PAYLOAD, jwt_secret.secret, 'HS256', header)
      local authorization = "Bearer "..jwt
      local response, status = http_client.get(STUB_GET_URL, nil, {host = "jwt.com", authorization = authorization})
      assert.equal(403, status)
      local body = json.decode(response)
      assert.equal("Invalid algorithm", body.message)
    end)
  end)

  describe("HS256", function()
    it("should proxy the request with token and consumer headers if it was verified", function()
      PAYLOAD.iss = jwt_secret.key
      local jwt = jwt_encoder.encode(PAYLOAD, jwt_secret.secret)
      local authorization = "Bearer "..jwt
      local response, status = http_client.get(STUB_GET_URL, nil, {host = "jwt.com", authorization = authorization})
      assert.equal(200, status)
      local body = json.decode(response)
      assert.equal(authorization, body.headers.authorization)
      assert.equal("jwt_tests_consumer", body.headers["x-consumer-username"])
    end)
    it("should proxy the request if secret key is stored in a field other than iss", function()
      PAYLOAD.aud = jwt_secret.key
      local jwt = jwt_encoder.encode(PAYLOAD, jwt_secret.secret)
      local authorization = "Bearer "..jwt
      local response, status = http_client.get(STUB_GET_URL, nil, {host = "jwt4.com", authorization = authorization})
      assert.equal(200, status)
      local body = json.decode(response)
      assert.equal(authorization, body.headers.authorization)
      assert.equal("jwt_tests_consumer", body.headers["x-consumer-username"])
    end)
    it("should proxy the request if secret is base64", function()
      PAYLOAD.iss = base64_jwt_secret.key
      local original_secret = base64_jwt_secret.secret
      local base64_secret = base64.encode(base64_jwt_secret.secret)
      local base_url = spec_helper.API_URL.."/consumers/jwt_tests_consumer/jwt/"..base64_jwt_secret.id
      http_client.patch(base_url, {key = base64_jwt_secret.key, secret = base64_secret})

      local jwt = jwt_encoder.encode(PAYLOAD, original_secret)
      local authorization = "Bearer "..jwt
      local response, status = http_client.get(STUB_GET_URL, nil, {host = "jwt5.com", authorization = authorization})
      assert.equal(200, status)
      local body = json.decode(response)
      assert.equal(authorization, body.headers.authorization)
      assert.equal("jwt_tests_consumer", body.headers["x-consumer-username"])
    end)
    it("should find the JWT if given in URL parameters", function()
      PAYLOAD.iss = jwt_secret.key
      local jwt = jwt_encoder.encode(PAYLOAD, jwt_secret.secret)
      local _, status = http_client.get(STUB_GET_URL.."?jwt="..jwt, nil, {host = "jwt.com"})
      assert.equal(200, status)
    end)
    it("should find the JWT if given in a custom URL parameter", function()
      PAYLOAD.iss = jwt_secret.key
      local jwt = jwt_encoder.encode(PAYLOAD, jwt_secret.secret)
      local _, status = http_client.get(STUB_GET_URL.."?token="..jwt, nil, {host = "jwt2.com"})
      assert.equal(200, status)
    end)
  end)

  describe("RS256", function()
    it("verifies JWT", function()
      PAYLOAD.iss = rsa_jwt_secret_1.key
      local jwt = jwt_encoder.encode(PAYLOAD, fixtures.rs256_private_key, 'RS256')
      local authorization = "Bearer "..jwt
      local response, status = http_client.get(STUB_GET_URL, nil, {host = "jwt.com", authorization = authorization})
      assert.equal(200, status)
      local body = json.decode(response)
      assert.equal(authorization, body.headers.authorization)
      assert.equal("jwt_tests_rsa_consumer_1", body.headers["x-consumer-username"])
    end)
    it("identifies Consumer", function()
      PAYLOAD.iss = rsa_jwt_secret_2.key
      local jwt = jwt_encoder.encode(PAYLOAD, fixtures.rs256_private_key, 'RS256')
      local authorization = "Bearer "..jwt
      local response, status = http_client.get(STUB_GET_URL, nil, {host = "jwt.com", authorization = authorization})
      assert.equal(200, status)
      local body = json.decode(response)
      assert.equal(authorization, body.headers.authorization)
      assert.equal("jwt_tests_rsa_consumer_2", body.headers["x-consumer-username"])
    end)
  end)

  describe("JWT private claims checks", function()
    it("should require the checked fields to be in the claims", function()
      local payload = {
        iss = jwt_secret.key
      }
      local jwt = jwt_encoder.encode(payload, jwt_secret.secret)
      local res, status = http_client.get(STUB_GET_URL.."?jwt="..jwt, nil, {host = "jwt3.com"})
      assert.equal(403, status)
      assert.equal('{"nbf":"must be a number","exp":"must be a number"}\n', res)
    end)
    it("should check if the fields are valid", function()
      local payload = {
        iss = jwt_secret.key,
        exp = os.time() - 10,
        nbf = os.time() - 10
      }
      local jwt = jwt_encoder.encode(payload, jwt_secret.secret)
      local res, status = http_client.get(STUB_GET_URL.."?jwt="..jwt, nil, {host = "jwt3.com"})
      assert.equal(403, status)
      assert.equal('{"exp":"token expired"}\n', res)
    end)
  end)
end)
