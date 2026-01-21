-- Wörterbuch WebSocket Protocol Dissector
-- Validates WebSocket text messages against client and server schemas
local worterbuch_ws_proto = Proto("worterbuch_ws", "Wörterbuch WebSocket Protocol")

-- Protocol fields
local f_message = ProtoField.string("worterbuch_ws.message", "JSON Message")
local f_message_type = ProtoField.string("worterbuch_ws.message_type", "Message Type")
local f_transaction_id = ProtoField.string("worterbuch_ws.transaction_id", "Transaction ID")
local f_invalid = ProtoField.string("worterbuch_ws.invalid", "Invalid Message")

worterbuch_ws_proto.fields = {
    f_message, f_message_type,
    f_transaction_id, f_invalid
}

-- Valid message types (combined list for heuristic detection)
local valid_message_types = {
    "welcome", "authorized", "state", "cState", "pState",
    "ack", "err", "lsState",
    "protocolSwitchRequest", "authorizationRequest", "get", "cGet", "pGet",
    "set", "cSet", "sPubInit", "sPub", "publish", "subscribe", "pSubscribe",
    "unsubscribe", "delete", "pDelete", "ls", "subscribeLs", "pLs",
    "unsubscribeLs", "lock", "acquireLock", "releaseLock"
}

-- Helper function to create a lookup table
local function create_lookup(list)
    local lookup = {}
    for _, v in ipairs(list) do
        lookup[v] = true
    end
    return lookup
end

local valid_types_lookup = create_lookup(valid_message_types)

-- Simple JSON message type extractor
local function extract_message_type(json_str)
    -- Remove leading/trailing whitespace
    json_str = json_str:match("^%s*(.-)%s*$")

    -- Must start with {
    if not json_str:match("^{") then
        return nil
    end

    -- Find first key
    local key = json_str:match('^%s*{%s*"([^"]+)"%s*:')

    return key
end

-- Extract transactionId if present
local function extract_transaction_id(json_str)
    local tid = json_str:match('"transactionId"%s*:%s*(%d+)')
    return tid
end

-- Parse message and extract type
local function parse_message(json_str)
    local msg_type = extract_message_type(json_str)

    if not msg_type then
        return false, nil, "Invalid JSON format"
    end

    return true, msg_type, nil
end

-- WebSocket opcode for text frames
local WS_TEXT_OPCODE = 1

-- Get the WebSocket opcode field
local ws_opcode_field = Field.new("websocket.opcode")

function worterbuch_ws_proto.dissector(buffer, pinfo, tree)
    local length = buffer:len()
    if length == 0 then return 0 end

    pinfo.cols.protocol = worterbuch_ws_proto.name

    local subtree = tree:add(worterbuch_ws_proto, buffer(), "Wörterbuch Protocol")

    -- Get the JSON payload
    local msg_text = buffer():string()

    -- Parse the message
    local is_valid, msg_type, error_msg = parse_message(msg_text)

    local msg_tree = subtree:add(f_message, buffer(), msg_text)

    if is_valid then
        msg_tree:add(f_message_type, msg_type)

        -- Extract transaction ID if present
        local tid = extract_transaction_id(msg_text)
        if tid then
            msg_tree:add(f_transaction_id, tid)
        end

        -- Update info column
        pinfo.cols.info:append(string.format(" [%s]", msg_type))
    else
        msg_tree:add_expert_info(PI_MALFORMED, PI_ERROR, error_msg or "Invalid message")
        msg_tree:add(f_invalid, error_msg or "Invalid message")
        if msg_type then
            msg_tree:add(f_message_type, msg_type .. " (invalid)")
        end

        -- Update info column
        pinfo.cols.info:append(" [Invalid]")
    end

    return length
end

-- Heuristic dissector function for WebSocket
local function heur_dissect_worterbuch_ws(buffer, pinfo, tree)
    local length = buffer:len()
    if length == 0 then return false end

    -- Check if this is a WebSocket text frame
    local ws_opcode = ws_opcode_field()
    if not ws_opcode or ws_opcode.value ~= WS_TEXT_OPCODE then
        return false
    end

    -- Get the payload as string
    local payload = buffer():string()

    -- Check if it looks like JSON
    if not payload:match("^%s*{") then
        return false
    end

    -- Try to extract message type
    local msg_type = extract_message_type(payload)
    if not msg_type then
        return false
    end

    -- Check if it's a valid Wörterbuch message type
    if not valid_types_lookup[msg_type] then
        return false
    end

    -- This looks like Wörterbuch, so dissect it
    worterbuch_ws_proto.dissector(buffer, pinfo, tree)
    return true
end

-- Register as a heuristic dissector for WebSocket
worterbuch_ws_proto:register_heuristic("ws", heur_dissect_worterbuch_ws)
