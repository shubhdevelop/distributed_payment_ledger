#!lua name=credits_lib

local function deductCredits(keys, args)
	local balKey = keys[1]
	local idempotency_key = keys[2]
	local streamKey = keys[3]

	local amt = tonumber(args[1])
	local txn_id = args[2]
	local user_id = args[3]

	if server.call("EXISTS", idempotency_key) == 1 then
		return { 1, "ALREADY_PROCESSED" }
	end

	local balance = server.call("GET", balKey)
	if not balance then
		return { -1, "CACHE_MISS" }
	end

	if tonumber(balance) < amt then
		return { -2, "INSUFFICIENT_BALANCE" }
	end

	local new_balance = server.call("DECRBY", balKey, amt)
	server.call("XADD", streamKey, "*", "user_id", user_id, "amount", -amt, "type", "deduct", "tx_id", txn_id)
	server.call("SETEX", idempotency_key, 86400, "OK")
	return { 1, "DEDUCTED", new_balance }
end

local function addCredits(keys, args)
	local balKey = keys[1]
	local idempotency_key = keys[2]
	local streamKey = keys[3]

	local amt = tonumber(args[1])
	local txn_id = args[2]
	local user_id = args[3]

	if server.call("EXISTS", idempotency_key) == 1 then
		return { 1, "ALREADY_PROCESSED" }
	end

	local balance = server.call("GET", balKey)
	if not balance then
		return { -1, "CACHE_MISS" }
	end

	local new_balance = server.call("INCRBY", balKey, amt)
	server.call("XADD", streamKey, "*", "user_id", user_id, "amount", amt, "type", "allot", "tx_id", txn_id)
	server.call("SETEX", idempotency_key, 86400, "OK")
	return { 1, "ALLOTED", new_balance }
end

local function getBalance(keys)
	local balKey = keys[1]

	local balance = server.call("GET", balKey)
	if not balance then
		return { -1, "CACHE_MISS" }
	end

	return { 1, "RETRIEVED", tonumber(balance) }
end

server.register_function("deductCredits", deductCredits)
server.register_function("addCredits", addCredits)
server.register_function("getBalance", getBalance)
