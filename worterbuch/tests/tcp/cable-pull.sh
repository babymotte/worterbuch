#!/bin/bash
sudo nft add table inet cablepull
sudo nft add chain inet cablepull in  '{ type filter hook input  priority -10; }'
sudo nft add chain inet cablepull out '{ type filter hook output priority -10; }'
sudo nft add rule inet cablepull in  tcp dport 1337 drop
sudo nft add rule inet cablepull in  tcp sport 1337 drop
sudo nft add rule inet cablepull out tcp dport 1337 drop
sudo nft add rule inet cablepull out tcp sport 1337 drop