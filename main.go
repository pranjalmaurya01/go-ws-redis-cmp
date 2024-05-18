package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

// Define the upgrader
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// Allow all connections by default
		return true
	},
}

var (
	fetchDataCh = make(chan struct{}, 1)
	connections sync.Map
)

func init() {
	// Start a goroutine to fetch Redis data every second
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Signal the need to fetch Redis data
				fetchDataCh <- struct{}{}
			}
		}
	}()
}

func fetchData() {
	for {
		select {
		case <-fetchDataCh:
			// Send Redis data to all connected clients
			connections.Range(func(key, value interface{}) bool {
				if conn, ok := key.(*websocket.Conn); ok {
					sendRedisData(conn)
				}
				return true
			})
		}
	}
}

func sendRedisData(conn *websocket.Conn) {
	rd := GetRedisData()

	// Write Redis data to the WebSocket connection
	err := conn.WriteMessage(websocket.TextMessage, rd)
	if err != nil {
		log.Println("Error writing message:", err)
		return
	}
}

// WebSocket endpoint
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Upgrade the HTTP connection to a WebSocket connection
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error during connection upgrade:", err)
		return
	}
	defer conn.Close()

	connections.Store(conn, struct{}{})
	log.Println("Client connected")

	sendRedisData(conn)

	// Keep the connection open
	for {
		// Read and discard any incoming messages from the client
		_, msg, err := conn.ReadMessage()
		log.Println(string(msg))
		if err != nil {
			log.Println("Error reading message:", err)
			break
		}
	}

	connections.Delete(conn)
	log.Println("Client dis-connected")
}

func GetRedisData() []byte {
	if err := godotenv.Load(); err != nil {
		panic("Error loading .env file")
	}

	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Password: os.Getenv("REDIS_PASS"),
		DB:       0,
	})

	// Define the cursor
	var cursor uint64
	var keys []string

	data := make(map[string]float64)

	for {
		// Scan with the cursor
		keys, cursor, _ = rdb.Scan(ctx, cursor, "*", 10).Result()
		// If cursor is 0, break the loop
		if cursor == 0 {
			break
		}
	}

	// pattern := "*"
	// keys, _ := rdb.Keys(ctx, pattern).Result()
	values, _ := rdb.MGet(ctx, keys...).Result()

	for i, key := range keys {
		data[key], _ = strconv.ParseFloat(values[i].(string), 64)
	}

	jsonData, _ := json.Marshal(data)
	return jsonData
}

func main() {
	go fetchData()

	// Set up the WebSocket endpoint
	http.HandleFunc("/", handleWebSocket)

	// Start the server on port 8080
	addr := "localhost:8080"
	log.Printf("Starting server on %s", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatalf("ListenAndServe error: %v", err)
	}
}
