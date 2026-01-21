-- JSON-over-TCP protocol dissector
local wb_proto = Proto("worterbuch", "Wörterbuch JSON Protocol")

-- Protocol fields
local f_message = ProtoField.string("worterbuch.message", "Wörterbuch Message")
local f_partial = ProtoField.string("worterbuch.partial", "Partial Message")
wb_proto.fields = {f_message, f_partial}

-- Preferences
wb_proto.prefs.port = Pref.uint("TCP Port", 8081, "The TCP port for this protocol")

function wb_proto.dissector(buffer, pinfo, tree)
    local buf_len = buffer:len()
    if buf_len == 0 then return end
    
    pinfo.cols.protocol = wb_proto.name
    
    local subtree = tree:add(wb_proto, buffer(), "Wörterbuch JSON Protocol")
    
    local offset = 0
    local message_count = 0
    
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
            
            subtree:add(f_message, msg_buf, msg_text)
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
        pinfo.cols.info = "1 JSON message"
    elseif message_count > 1 then
        pinfo.cols.info = string.format("%d JSON messages", message_count)
    end
end

-- Register the dissector on TCP port
local tcp_table = DissectorTable.get("tcp.port")
tcp_table:add(wb_proto.prefs.port, wb_proto)

-- Allow dynamic port changes
function wb_proto.init()
    tcp_table:add(wb_proto.prefs.port, wb_proto)
end