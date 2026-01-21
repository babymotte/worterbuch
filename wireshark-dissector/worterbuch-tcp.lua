-- Wörterbuch Protocol Dissector
-- Validates messages against client and server schemas
local worterbuch_tcp_proto = Proto("worterbuch_tcp", "Wörterbuch Protocol")

-- Protocol fields
local f_message = ProtoField.string("worterbuch_tcp.message", "JSON Message")
local f_partial = ProtoField.string("worterbuch_tcp.partial", "Partial Message")
local f_message_type = ProtoField.string("worterbuch_tcp.message_type", "Message Type")
local f_direction = ProtoField.string("worterbuch_tcp.direction", "Direction")
local f_transaction_id = ProtoField.string("worterbuch_tcp.transaction_id", "Transaction ID")
local f_validation = ProtoField.string("worterbuch_tcp.validation", "Validation")
local f_invalid = ProtoField.string("worterbuch_tcp.invalid", "Invalid Message")
worterbuch_tcp_proto.fields = {
    f_message, f_partial, f_message_type, f_direction,
    f_transaction_id, f_validation, f_invalid
}

-- Preferences
worterbuch_tcp_proto.prefs.port = Pref.uint("TCP Port", 8081, "The TCP port for this protocol")

-- Valid message types for each direction
local server_message_types = {
    "welcome", "authorized", "state", "cState", "pState",
    "ack", "err", "lsState"
}

local client_message_types = {
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

local server_types_lookup = create_lookup(server_message_types)
local client_types_lookup = create_lookup(client_message_types)

-- Simple JSON message type extractor
-- Extracts the first top-level key from a JSON object
local function extract_message_type(json_str)
    -- Remove leading/trailing whitespace
    json_str = json_str:match("^%s*(.-)%s*$")

    -- Must start with {
    if not json_str:match("^{") then
        return nil
    end

    -- Find first key (handling both "key" and key formats)
    -- Pattern: optional whitespace, quote, key name, quote, colon
    local key = json_str:match('^%s*{%s*"([^"]+)"%s*:')

    return key
end

-- Extract transactionId if present
local function extract_transaction_id(json_str)
    local tid = json_str:match('"transactionId"%s*:%s*(%d+)')
    return tid
end

-- Validate message based on direction
local function validate_message(json_str, is_client_to_server)
    local msg_type = extract_message_type(json_str)

    if not msg_type then
        return false, nil, "Invalid JSON format"
    end

    local valid_types = is_client_to_server and client_types_lookup or server_types_lookup
    local expected_direction = is_client_to_server and "Client→Server" or "Server→Client"

    if not valid_types[msg_type] then
        local actual_direction = client_types_lookup[msg_type] and "Client→Server" or
            (server_types_lookup[msg_type] and "Server→Client" or "Unknown")
        if actual_direction ~= "Unknown" and actual_direction ~= expected_direction then
            return false, msg_type, string.format("Wrong direction (expected %s, got %s)",
                expected_direction, actual_direction)
        else
            return false, msg_type, "Unknown message type"
        end
    end

    return true, msg_type, nil
end

function worterbuch_tcp_proto.dissector(buffer, pinfo, tree)
    local buf_len = buffer:len()
    if buf_len == 0 then return end

    pinfo.cols.protocol = worterbuch_tcp_proto.name

    local subtree = tree:add(worterbuch_tcp_proto, buffer(), "Wörterbuch Protocol")

    -- Determine direction based on port
    local is_client_to_server = (pinfo.dst_port == worterbuch_tcp_proto.prefs.port)
    local direction = is_client_to_server and "Client → Server" or "Server → Client"

    local offset = 0
    local message_count = 0
    local valid_count = 0
    local invalid_count = 0

    while offset < buf_len do
        -- Look for newline
        local newline_pos = nil
        for i = offset, buf_len - 1 do
            if buffer(i, 1):uint() == 10 then -- \n
                newline_pos = i
                break
            end
        end

        if newline_pos then
            -- Complete message found
            local msg_len = newline_pos - offset + 1
            local msg_buf = buffer(offset, msg_len)
            local msg_text = msg_buf:string():gsub("\n$", "") -- Remove trailing newline

            -- Validate the message
            local is_valid, msg_type, error_msg = validate_message(msg_text, is_client_to_server)

            local msg_tree = subtree:add(f_message, msg_buf, msg_text)
            msg_tree:add(f_direction, direction)

            if is_valid then
                msg_tree:add(f_message_type, msg_type)
                msg_tree:add(f_validation, "Valid")

                -- Extract transaction ID if present
                local tid = extract_transaction_id(msg_text)
                if tid then
                    msg_tree:add(f_transaction_id, tid)
                end

                valid_count = valid_count + 1
            else
                msg_tree:add_expert_info(PI_MALFORMED, PI_ERROR, error_msg or "Invalid message")
                msg_tree:add(f_invalid, error_msg or "Invalid message")
                if msg_type then
                    msg_tree:add(f_message_type, msg_type .. " (invalid)")
                end
                invalid_count = invalid_count + 1
            end

            message_count = message_count + 1
            offset = newline_pos + 1
        else
            -- Incomplete message - need more data
            local remaining = buffer(offset, buf_len - offset)
            subtree:add(f_partial, remaining, remaining:string())

            -- Request reassembly
            pinfo.desegment_offset = offset
            pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
            return
        end
    end

    -- Update info column
    if message_count == 1 then
        local is_valid, msg_type = validate_message(buffer():string():gsub("\n$", ""), is_client_to_server)
        if is_valid then
            pinfo.cols.info = string.format("%s: %s", direction, msg_type)
        else
            pinfo.cols.info = string.format("%s: Invalid message", direction)
        end
    elseif message_count > 1 then
        if invalid_count > 0 then
            pinfo.cols.info = string.format("%s: %d messages (%d valid, %d invalid)",
                direction, message_count, valid_count, invalid_count)
        else
            pinfo.cols.info = string.format("%s: %d messages", direction, message_count)
        end
    end
end

-- Register the dissector on TCP port
local tcp_table = DissectorTable.get("tcp.port")
tcp_table:add(worterbuch_tcp_proto.prefs.port, worterbuch_tcp_proto)

-- Allow dynamic port changes
function worterbuch_tcp_proto.init()
    tcp_table:add(worterbuch_tcp_proto.prefs.port, worterbuch_tcp_proto)
end
