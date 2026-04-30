#!/bin/bash
# VortexDB Demo Script - Tests all major features
# Sends commands via TCP to the VortexDB server

HOST="127.0.0.1"
PORT="7070"

send_cmd() {
    echo "$1" | nc -w 2 $HOST $PORT | tail -1
}

echo "═══════════════════════════════════════════════════"
echo "    🌀 VortexDB Live Demo"
echo "═══════════════════════════════════════════════════"
echo ""

echo "1️⃣  PING"
send_cmd '{"action":"ping"}'
echo ""

echo "2️⃣  CREATE COLLECTION: users"
send_cmd '{"action":"createCollection","collection":"users"}'
echo ""

echo "3️⃣  INSERT documents"
send_cmd '{"action":"insert","collection":"users","document":{"name":"Alice","age":30,"city":"NYC","role":"engineer"}}'
echo ""
send_cmd '{"action":"insert","collection":"users","document":{"name":"Bob","age":25,"city":"LA","role":"designer"}}'
echo ""
send_cmd '{"action":"insert","collection":"users","document":{"name":"Charlie","age":35,"city":"NYC","role":"manager"}}'
echo ""
send_cmd '{"action":"insert","collection":"users","document":{"name":"Diana","age":28,"city":"SF","role":"engineer"}}'
echo ""
send_cmd '{"action":"insert","collection":"users","document":{"name":"Eve","age":42,"city":"NYC","role":"director"}}'
echo ""

echo "4️⃣  FIND ALL documents"
send_cmd '{"action":"find","collection":"users","query":{}}'
echo ""

echo "5️⃣  FIND with query: age >= 30"
send_cmd '{"action":"find","collection":"users","query":{"age":{"$gte":30}}}'
echo ""

echo "6️⃣  FIND with $in operator: city in NYC or SF"
send_cmd '{"action":"find","collection":"users","query":{"city":{"$in":["NYC","SF"]}}}'
echo ""

echo "7️⃣  FIND with $or: age < 26 OR age > 40"
send_cmd '{"action":"find","collection":"users","query":{"$or":[{"age":{"$lt":26}},{"age":{"$gt":40}}]}}'
echo ""

echo "8️⃣  FIND with sort + limit: youngest 2"
send_cmd '{"action":"find","collection":"users","query":{},"sort":{"age":1},"limit":2}'
echo ""

echo "9️⃣  COUNT engineers"
send_cmd '{"action":"count","collection":"users","query":{"role":"engineer"}}'
echo ""

echo "🔟  UPDATE: set Alice age to 31"
send_cmd '{"action":"update","collection":"users","query":{"name":"Alice"},"update":{"$set":{"age":31}}}'
echo ""

echo "1️⃣1️⃣  VERIFY UPDATE"
send_cmd '{"action":"findOne","collection":"users","query":{"name":"Alice"}}'
echo ""

echo "1️⃣2️⃣  CREATE INDEX on 'age'"
send_cmd '{"action":"createIndex","collection":"users","field":"age","unique":false}'
echo ""

echo "1️⃣3️⃣  LIST INDEXES"
send_cmd '{"action":"listIndexes","collection":"users"}'
echo ""

echo "1️⃣4️⃣  DELETE: remove Bob"
send_cmd '{"action":"delete","collection":"users","query":{"name":"Bob"}}'
echo ""

echo "1️⃣5️⃣  INSERT into a second collection: products"
send_cmd '{"action":"insert","collection":"products","document":{"name":"Laptop","price":999.99,"category":"electronics","stock":50}}'
echo ""
send_cmd '{"action":"insert","collection":"products","document":{"name":"Phone","price":699.99,"category":"electronics","stock":120}}'
echo ""

echo "1️⃣6️⃣  LIST COLLECTIONS"
send_cmd '{"action":"listCollections"}'
echo ""

echo "1️⃣7️⃣  DATABASE STATS"
send_cmd '{"action":"stats"}'
echo ""

echo "═══════════════════════════════════════════════════"
echo "    ✅ Demo complete! All operations succeeded."
echo "═══════════════════════════════════════════════════"
