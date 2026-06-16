-- worterbuch-cluster-orchestrator.lua
-- Wireshark dissector for Wörterbuch Cluster PeerMessage (JSON over UDP)
--
-- Wire format: raw UTF-8 JSON in a single UDP datagram, no length prefix.
-- Serde serialization uses external tagging + camelCase variant names:
--
--   Heartbeat Request:  {"heartbeat":{"request":{"nodeId":"<id>"}}}
--   Heartbeat Response: {"heartbeat":{"response":{"nodeId":"<id>"}}}
--   Vote Request:       {"vote":{"request":{"nodeId":"<id>","priority":<i64>}}}
--   Vote Response:      {"vote":{"response":{"nodeId":"<id>"}}}
--
-- Installation:
--   Symlink or copy to ~/.local/lib/wireshark/plugins/  (Linux)
--   Then restart Wireshark — Reload Lua Plugins will NOT work due to a
--   Wireshark limitation (Proto objects cannot be unregistered between reloads).
--
-- This dissector registers as a post-dissector so it runs on every packet
-- without requiring any port configuration or heuristic toggles in the UI.

local proto = Proto("wb_peer", "Wörterbuch Cluster PeerMessage")

-- ── Protocol fields ──────────────────────────────────────────────────────────

local f_type     = ProtoField.string("wb_peer.type",     "Message Type")
local f_subtype  = ProtoField.string("wb_peer.subtype",  "Subtype")
local f_node_id  = ProtoField.string("wb_peer.node_id",  "Node ID")
local f_priority = ProtoField.string("wb_peer.priority", "Priority")
local f_raw      = ProtoField.string("wb_peer.raw",      "Raw JSON")

proto.fields = { f_type, f_subtype, f_node_id, f_priority, f_raw }

-- ── Field accessors (must be declared at module level) ───────────────────────

-- Used to gate the post-dissector to UDP packets only.
local f_udp_src     = Field.new("udp.srcport")
-- The UDP payload as a TvbRange; present once the UDP dissector has run.
local f_udp_payload = Field.new("udp.payload")
-- Fallback for ports not claimed by any other dissector (shown as raw "Data").
local f_data        = Field.new("data.data")

-- ── Helpers ──────────────────────────────────────────────────────────────────

local function json_get(s, key)
    local val = s:match('"' .. key .. '"%s*:%s*"([^"]*)"')
    if not val then
        val = s:match('"' .. key .. '"%s*:%s*(%-?%d+)')
    end
    return val
end

local function looks_like_wb_peer(s)
    return s:match("^%s*{") and (s:match('"heartbeat"') or s:match('"vote"'))
end

-- ── Core dissection logic ────────────────────────────────────────────────────

local function do_dissect(range, pinfo, tree)
    local ok, payload = pcall(function() return range:string() end)
    if not ok or not looks_like_wb_peer(payload) then return end

    pinfo.cols.protocol:set("WB-PEER")

    local root = tree:add(proto, range, "Wörterbuch PeerMessage")
    root:add(f_raw, range, payload)

    local msg_type, msg_subtype, node_id, priority

    if payload:match('"heartbeat"') then
        msg_type = "Heartbeat"
        if payload:match('"request"') then
            msg_subtype = "Request"
        elseif payload:match('"response"') then
            msg_subtype = "Response"
        end
    elseif payload:match('"vote"') then
        msg_type = "Vote"
        if payload:match('"request"') then
            msg_subtype = "Request"
            priority = json_get(payload, "priority")
        elseif payload:match('"response"') then
            msg_subtype = "Response"
        end
    end

    node_id = json_get(payload, "nodeId")

    if msg_type    then root:add(f_type,     range, msg_type)    end
    if msg_subtype then root:add(f_subtype,  range, msg_subtype) end
    if node_id     then root:add(f_node_id,  range, node_id)     end
    if priority    then root:add(f_priority, range, priority)    end

    local info = (msg_type or "?") .. " " .. (msg_subtype or "?")
    if node_id  then info = info .. "  node=" .. node_id  end
    if priority then info = info .. "  priority=" .. priority end
    pinfo.cols.info:set(info)
end

-- ── Post-dissector registration ───────────────────────────────────────────────
--
-- A post-dissector is called after all normal dissectors on every packet.
-- No port configuration or heuristic toggles are needed.

register_postdissector(proto)

function proto.dissector(_, pinfo, tree)
    -- Gate to UDP packets only
    if not f_udp_src() then return end

    -- Prefer the udp.payload field (Wireshark 3.5+); fall back to data.data
    -- for ports not claimed by another dissector.
    local fis = { f_udp_payload() }
    if #fis == 0 then
        fis = { f_data() }
    end
    if #fis == 0 then return end

    do_dissect(fis[1].range, pinfo, tree)
end
