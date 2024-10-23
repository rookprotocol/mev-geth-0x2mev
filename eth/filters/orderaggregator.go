package filters

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/go-redis/redis/v8"
)

// TODO nick clean up the file
type Order struct {
	OrderHash     string      `json:"orderHash"`
	OrderBookName string      `json:"orderBookName"`
	OffChainData  interface{} `json:"offChainData"`
	OnChainData   OnChainData `json:"onChainData,omitempty"`
}

type OnChainData struct {
	OrderInfo               interface{} `json:"orderInfo"`
	MakerBalance_weiUnits   *big.Int    `json:"makerBalance_weiUnits"`
	MakerAllowance_weiUnits *big.Int    `json:"makerAllowance_weiUnits"`
}

var (
	ctx       = context.Background()
	sharedRdb *redis.Client
	lastID    = "0"
	mu        sync.Mutex
	// In-memory data structure to store order data, keyed by orderHash
	orderDataStore = make(map[string]Order)
)

func initRedis() {
	sharedRdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func cleanUpRedisStreams() {
	// Clean up Redis streams
	sharedRdb.Del(ctx, "snapshotStream")
	sharedRdb.Del(ctx, "updateStream")
}

func fetchSnapshot() {
	log.Println("fetchSnapshot: fetching initial snapshot")
	for {
		// Fetch the latest snapshot from shared Redis using XRevRange
		snapshot, err := sharedRdb.XRevRangeN(ctx, "snapshotStream", "+", "-", 1).Result()
		if err != nil {
			log.Printf("fetchSnapshot: Failed to read snapshot: %v", err)
			time.Sleep(5 * time.Second) // Retry after 5 seconds
			continue
		}

		if len(snapshot) == 0 {
			log.Println("fetchSnapshot: No snapshot found, retrying...")
			time.Sleep(5 * time.Second) // Retry after 5 seconds
			continue
		}

		// Initialize local state with the snapshot
		fmt.Println("fetchSnapshot: Snapshot:", snapshot)
		snapshotData := snapshot[0].Values["snapshot"].(string)
		err = json.Unmarshal([]byte(snapshotData), &orderDataStore)
		if err != nil {
			log.Printf("fetchSnapshot: Failed to unmarshal snapshot data: %v", err)
			time.Sleep(5 * time.Second) // Retry after 5 seconds
			continue
		}

		// Update the last_id to the ID of the snapshot
		lastID = snapshot[0].ID
		break
	}
}

func processExistingOrders() {
	log.Println("processExistingOrders: processing existing orders")
	for orderHash, orderData := range orderDataStore {
		// Process the order data
		fmt.Printf("processExistingOrders: Processing order data for %s: %v\n", orderHash, orderData)
		updateOrdersOnchainData(orderHash)
	}
	log.Println("processExistingOrders: all existing orders processed")
}

func updateOrdersOnchainData(orderHash string) {
	// Retrieve existing order data
	mu.Lock()
	order := orderDataStore[orderHash]
	mu.Unlock()
	// log.Println("updateOrdersOnchainData: order:", order)

	// Handle missing or empty fields
	if order.OnChainData.MakerAllowance_weiUnits == nil || order.OnChainData.MakerBalance_weiUnits == nil || order.OnChainData.OrderInfo == nil {
		switch order.OrderBookName {
		case ORDERBOOKNAME_ZRX:
			zrxOrder, err := ZRXConvertOrderToZRXOrder(order)
			if err != nil {
				log.Printf("updateOrdersOnchainData: Failed to convert order to ZRXOrder: %v", err)
				return
			}
			onChainData, err := ZRXGetOnChainData(zrxOrder)
			if err != nil {
				log.Printf("updateOrdersOnchainData: Failed to get ZRX on-chain data: %v", err)
				return
			}
			order.OnChainData = onChainData
		case ORDERBOOKNAME_TEMPO:
			tempoOrder, err := TempoConvertOrderToTempoOrder(order)
			if err != nil {
				log.Printf("updateOrdersOnchainData: Failed to convert order to TempoOrder: %v", err)
				return
			}
			onChainData, err := TempoGetOnChainData(tempoOrder)
			if err != nil {
				log.Printf("updateOrdersOnchainData: Failed to get Tempo on-chain data: %v", err)
				return
			}
			order.OnChainData = onChainData
		// Add cases for other order books here
		default:
			log.Printf("updateOrdersOnchainData: Unknown order book name: %s", order.OrderBookName)
			return
		}

		// Write the update back to the stream if needed
		update := map[string]interface{}{
			"orderHash":   orderHash,
			"onChainData": order.OnChainData,
		}
		writeUpdateToStream(update)
	}
}

func convertValuesToStringsAndRemoveScientificNotation(data map[string]interface{}) map[string]interface{} {
    for key, value := range data {
        switch v := value.(type) {
        case map[string]interface{}:
            data[key] = convertValuesToStringsAndRemoveScientificNotation(v)
        case []interface{}:
            for i, item := range v {
                if itemMap, ok := item.(map[string]interface{}); ok {
                    v[i] = convertValuesToStringsAndRemoveScientificNotation(itemMap)
                } else {
                    v[i] = fmt.Sprintf("%v", item)
                }
            }
            data[key] = v
        case float64:
            data[key] = fmt.Sprintf("%.0f", v)
        case int:
            data[key] = fmt.Sprintf("%d", v)
        default:
            data[key] = fmt.Sprintf("%v", value)
        }
    }
    return data
}

func processUpdates() {
	for {
		// Fetch updates from Redis
		updates, err := sharedRdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"updateStream", lastID},
			Block:   0, // Blocking indefinitely for new updates
		}).Result()
		if err != nil {
			log.Fatalf("processUpdates: Failed to read updates: %v", err)
		}

		if len(updates) == 0 || len(updates[0].Messages) == 0 {
			log.Println("processUpdates: No updates found")
			continue
		}

		updateLoop:
		for _, update := range updates[0].Messages {
			lastID = update.ID

			// Create a new Order object to hold the update
			var orderUpdate Order

			// Deserialize the "data" field into a map
			var updateData map[string]interface{}
			if err := json.Unmarshal([]byte(update.Values["data"].(string)), &updateData); err != nil {
				log.Printf("processUpdates: Failed to unmarshal update data: %v", err)
				continue
			}

			// Convert all values in the map to strings
			updateData = convertValuesToStringsAndRemoveScientificNotation(updateData)

			// Iterate over the key-value pairs in the update
			var hasOffChainData bool = false
			for key, value := range updateData {
				switch key {
				case "orderHash":
					orderUpdate.OrderHash = value.(string)
				case "orderBookName":
					orderUpdate.OrderBookName = value.(string)
				case "offChainData":
					orderUpdate.OffChainData = value
					hasOffChainData = true
				case "deleted":
					// if the order is deleted, remove it from the orderDataStore
					if deleted, ok := value.(string); ok && strings.ToLower(deleted) == "true" {
						mu.Lock()
						delete(orderDataStore, orderUpdate.OrderHash)
						mu.Unlock()
						break updateLoop
					}
				}
				
			}
			// if updateData lacks orderHash, skip the update
			if orderUpdate.OrderHash == "" {
				log.Println("processUpdates: orderHash not found, skipping")
				continue
			}
			if !hasOffChainData {
				continue
			}

			// Retrieve existing order data or create a new entry if it doesn't exist
			mu.Lock()
			order, exists := orderDataStore[orderUpdate.OrderHash]
			if !exists {
				order = Order{OrderHash: orderUpdate.OrderHash}
			}

			if orderUpdate.OffChainData != nil {
				order.OffChainData = orderUpdate.OffChainData
			}
			if orderUpdate.OrderBookName != "" {
				order.OrderBookName = orderUpdate.OrderBookName
			}

			// Update the in-memory data store
			orderDataStore[order.OrderHash] = order
			mu.Unlock()

			updateOrdersOnchainData(order.OrderHash)
		}

		// this is required to release the lock to create the snapshot.
		// we might want to keep a timeout here on the nodes as well
		time.Sleep(50 * time.Millisecond)
	}

}

// func writeUpdateToStream(updateData interface{}) {
// 	// Serialize the update data to JSON
// 	jsonData, err := json.Marshal(updateData)
// 	if err != nil {
// 		log.Fatalf("Failed to marshal update data: %v", err)
// 	}

// 	// Write update back to the shared Redis stream
// 	_, err = sharedRdb.XAdd(ctx, &redis.XAddArgs{
// 		Stream: "updateStream",
// 		Values: map[string]interface{}{"data": string(jsonData)},
// 	}).Result()
// 	if err != nil {
// 		log.Fatalf("writeUpdateToStream: Failed to write update: %v", err)
// 	}
// 	log.Println("writeUpdateToStream: update written to stream")
// }

func writeUpdateToStream(updateMap map[string]interface{}) error {
    // Convert the updateMap to a byte slice
    data, err := json.Marshal(updateMap)
    if err != nil {
        return fmt.Errorf("failed to marshal updateMap: %v", err)
    }

    // Write the data to the Redis stream
	err = sharedRdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "updateStream",
		Values: map[string]interface{}{
			"data": data,
		},
	}).Err()
    if err != nil {
        return fmt.Errorf("failed to write update to stream: %v", err)
    }

    return nil
}

func createSnapshot() {
	log.Println("createSnapshot: waiting for lock")
	mu.Lock()
	defer mu.Unlock()
	log.Println("createSnapshot: creating snapshot...")

	// Serialize the current state of the orderDataStore
	log.Println("createSnapshot: orderDataStore", orderDataStore)
	snapshotData, err := json.Marshal(orderDataStore)
	if err != nil {
		log.Fatalf("Failed to marshal snapshot data: %v", err)
	}

	// Write the snapshot to the snapshotStream
	_, err = sharedRdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "snapshotStream",
		Values: map[string]interface{}{"snapshot": string(snapshotData)},
	}).Result()
	if err != nil {
		log.Fatalf("createSnapshot: Failed to create snapshot: %v", err)
	}
	log.Println("createSnapshot: snapshot created")
}

func StartOrderBookAggregatorService() {

	WaitForHTTPServerToStart()

	log.Println("StartOrderBookAggregatorService: orderbook aggregator started")

	initRedis()

	log.Println("StartOrderBookAggregatorService: redis initialized")

	// cleanUpRedisStreams()

	// ZRXCreateOrder()

	// Create an initial snapshot if none exists
	// createSnapshot()

	// Fetch initial snapshot and initialize local state
	// fetchSnapshot()
	log.Println("StartOrderBookAggregatorService: initial snapshot fetched")

	// Start a goroutine to process updates continuously
	// processExistingOrders()
	go processUpdates()
	// log.Println("process updates started")

	time.Sleep(100 * time.Millisecond)
	log.Println("StartOrderBookAggregatorService: going to create a snapshot")
	createSnapshot()
	log.Println("StartOrderBookAggregatorService: last snapshot created")

	// // Keep the main function running to allow the goroutine to process updates
	select {}
}

func WaitForHTTPServerToStart() {
	// doing a random call until we get a valid response to know that the server has started
	log.Println("StartOrderBookAggregatorService: waiting for http server to start...")
	for {
		balance, err := GetERC20TokenBalance(
			common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
			common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"))
		if err == nil {
			log.Println("StartOrderBookAggregatorService: http server started")
			log.Println("StartOrderBookAggregatorService: balance", balance)
			break
		}
		time.Sleep(5 * time.Second)
	}
}

func GetBalanceMetaData_OrderBooks(address common.Address, eventLog *Log) (interface{}, error) {
	switch address {
	case ORDERBOOKADDRESS_ZRX:
		return GetBalanceMetaData_Zrx(address, eventLog)
	case ORDERBOOKADDRESS_TEMPO:
		return GetBalanceMetaData_Tempo(address, eventLog)
	// Add cases for other order books here
	default:
		return "", fmt.Errorf("address not implemented in GetBalanceMetaData_OrderBook: %s", address.Hex())
	}
}
