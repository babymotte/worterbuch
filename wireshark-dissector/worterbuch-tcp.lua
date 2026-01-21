-- Wörterbuch Protocol Dissector
-- Validates messages against client and server schemas
local worterbuch_tcp_proto = Proto("worterbuch_tcp", "Wörterbuch Protocol")

-- Protocol fields
local f_message = ProtoField.string("worterbuch_tcp.message", "JSON Message")
local f_partial = ProtoField.string("worterbuch_tcp.partial", "Partial Message")
local f_message_type = ProtoField.string("worterbuch_tcp.message_type", "Message Type")
local f_src_port = ProtoField.uint16("worterbuch_tcp.src_port", "Source Port")
local f_dst_port = ProtoField.uint16("worterbuch_tcp.dst_port", "Destination Port")
local f_transaction_id = ProtoField.string("worterbuch_tcp.transaction_id", "Transaction ID")
local f_invalid = ProtoField.string("worterbuch_tcp.invalid", "Invalid Message")
worterbuch_tcp_proto.fields = {
    f_message, f_partial, f_message_type, f_src_port, f_dst_port,
    f_transaction_id, f_invalid
}

-- Preferences
worterbuch_tcp_proto.prefs.port = Pref.uint("TCP Port", 8081, "The TCP port for this protocol")

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

-- Parse message and extract type
local function parse_message(json_str)
    local msg_type = extract_message_type(json_str)

    if not msg_type then
        return false, nil, "Invalid JSON format"
    end

    return true, msg_type, nil
end

function worterbuch_tcp_proto.dissector(buffer, pinfo, tree)
    local buf_len = buffer:len()
    if buf_len == 0 then return end

    pinfo.cols.protocol = worterbuch_tcp_proto.name

    local subtree = tree:add(worterbuch_tcp_proto, buffer(), "Wörterbuch Protocol")

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

            -- Parse the message
            local is_valid, msg_type, error_msg = parse_message(msg_text)

            local msg_tree = subtree:add(f_message, msg_buf, msg_text)

            if is_valid then
                msg_tree:add(f_message_type, msg_type)

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
    local port_info = string.format("%d → %d", pinfo.src_port, pinfo.dst_port)

    if message_count == 1 then
        local is_valid, msg_type = parse_message(buffer():string():gsub("\n$", ""))
        if is_valid then
            pinfo.cols.info = string.format("%s: %s", port_info, msg_type)
        else
            pinfo.cols.info = string.format("%s: Invalid message", port_info)
        end
    elseif message_count > 1 then
        if invalid_count > 0 then
            pinfo.cols.info = string.format("%s: %d messages (%d valid, %d invalid)",
                port_info, message_count, valid_count, invalid_count)
        else
            pinfo.cols.info = string.format("%s: %d messages", port_info, message_count)
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
