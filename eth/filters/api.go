// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package filters

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

var (
	errInvalidTopic           = errors.New("invalid topic(s)")
	errFilterNotFound         = errors.New("filter not found")
	errInvalidBlockRange      = errors.New("invalid block range params")
	errPendingLogsUnsupported = errors.New("pending logs are not supported")
	errExceedMaxTopics        = errors.New("exceed max topics")
)

// The maximum number of topic criteria allowed, vm.LOG4 - vm.LOG0
const maxTopics = 4

// The maximum number of allowed topics within a topic criteria
const maxSubTopics = 1000

// filter is a helper struct that holds meta information over the filter type
// and associated subscription in the event system.
type filter struct {
	typ      Type
	deadline *time.Timer // filter is inactive when deadline triggers
	hashes   []common.Hash
	fullTx   bool
	txs      []*types.Transaction
	crit     FilterCriteria
	logs     []*types.Log
	s        *Subscription // associated subscription in event system
}

// FilterAPI offers support to create and manage filters. This will allow external clients to retrieve various
// information related to the Ethereum protocol such as blocks, transactions and logs.
type FilterAPI struct {
	sys       *FilterSystem
	events    *EventSystem
	filtersMu sync.Mutex
	filters   map[rpc.ID]*filter
	timeout   time.Duration
}

// NewFilterAPI returns a new FilterAPI instance.
func NewFilterAPI(system *FilterSystem) *FilterAPI {
	api := &FilterAPI{
		sys:     system,
		events:  NewEventSystem(system),
		filters: make(map[rpc.ID]*filter),
		timeout: system.cfg.Timeout,
	}
	go api.timeoutLoop(system.cfg.Timeout)

	return api
}

// timeoutLoop runs at the interval set by 'timeout' and deletes filters
// that have not been recently used. It is started when the API is created.
func (api *FilterAPI) timeoutLoop(timeout time.Duration) {
	var toUninstall []*Subscription
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()
	for {
		<-ticker.C
		api.filtersMu.Lock()
		for id, f := range api.filters {
			select {
			case <-f.deadline.C:
				toUninstall = append(toUninstall, f.s)
				delete(api.filters, id)
			default:
				continue
			}
		}
		api.filtersMu.Unlock()

		// Unsubscribes are processed outside the lock to avoid the following scenario:
		// event loop attempts broadcasting events to still active filters while
		// Unsubscribe is waiting for it to process the uninstall request.
		for _, s := range toUninstall {
			s.Unsubscribe()
		}
		toUninstall = nil
	}
}

// NewPendingTransactionFilter creates a filter that fetches pending transactions
// as transactions enter the pending state.
//
// It is part of the filter package because this filter can be used through the
// `eth_getFilterChanges` polling method that is also used for log filters.
func (api *FilterAPI) NewPendingTransactionFilter(fullTx *bool) rpc.ID {
	var (
		pendingTxs   = make(chan []*types.Transaction)
		pendingTxSub = api.events.SubscribePendingTxs(pendingTxs)
	)

	api.filtersMu.Lock()
	api.filters[pendingTxSub.ID] = &filter{typ: PendingTransactionsSubscription, fullTx: fullTx != nil && *fullTx, deadline: time.NewTimer(api.timeout), txs: make([]*types.Transaction, 0), s: pendingTxSub}
	api.filtersMu.Unlock()

	go func() {
		for {
			select {
			case pTx := <-pendingTxs:
				api.filtersMu.Lock()
				if f, found := api.filters[pendingTxSub.ID]; found {
					f.txs = append(f.txs, pTx...)
				}
				api.filtersMu.Unlock()
			case <-pendingTxSub.Err():
				api.filtersMu.Lock()
				delete(api.filters, pendingTxSub.ID)
				api.filtersMu.Unlock()
				return
			}
		}
	}()

	return pendingTxSub.ID
}

// NewPendingTransactions creates a subscription that is triggered each time a
// transaction enters the transaction pool. If fullTx is true the full tx is
// sent to the client, otherwise the hash is sent.
func (api *FilterAPI) NewPendingTransactions(ctx context.Context, fullTx *bool) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		txs := make(chan []*types.Transaction, 128)
		pendingTxSub := api.events.SubscribePendingTxs(txs)
		defer pendingTxSub.Unsubscribe()

		chainConfig := api.sys.backend.ChainConfig()

		for {
			select {
			case txs := <-txs:
				// To keep the original behaviour, send a single tx hash in one notification.
				// TODO(rjl493456442) Send a batch of tx hashes in one notification
				latest := api.sys.backend.CurrentHeader()
				for _, tx := range txs {
					if fullTx != nil && *fullTx {
						rpcTx := ethapi.NewRPCPendingTransaction(tx, latest, chainConfig)
						notifier.Notify(rpcSub.ID, rpcTx)
					} else {
						notifier.Notify(rpcSub.ID, tx.Hash())
					}
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// NewBlockFilter creates a filter that fetches blocks that are imported into the chain.
// It is part of the filter package since polling goes with eth_getFilterChanges.
func (api *FilterAPI) NewBlockFilter() rpc.ID {
	var (
		headers   = make(chan *types.Header)
		headerSub = api.events.SubscribeNewHeads(headers)
	)

	api.filtersMu.Lock()
	api.filters[headerSub.ID] = &filter{typ: BlocksSubscription, deadline: time.NewTimer(api.timeout), hashes: make([]common.Hash, 0), s: headerSub}
	api.filtersMu.Unlock()

	go func() {
		for {
			select {
			case h := <-headers:
				api.filtersMu.Lock()
				if f, found := api.filters[headerSub.ID]; found {
					f.hashes = append(f.hashes, h.Hash())
				}
				api.filtersMu.Unlock()
			case <-headerSub.Err():
				api.filtersMu.Lock()
				delete(api.filters, headerSub.ID)
				api.filtersMu.Unlock()
				return
			}
		}
	}()

	return headerSub.ID
}

type PoolBalanceMetaData struct {
	ExchangeName    string
	Address         common.Address
	Topic           common.Hash
	BalanceMetaData interface{}
}

type NewHeadsWithPoolBalanceMetaData struct {
	Header              *types.Header
	PoolBalanceMetaData map[common.Address]PoolBalanceMetaData
}

var exchangeName_UniswapV2 string
var exchangeName_UniswapV3 string
var exchangeName_BalancerV2 string
var exchangeName_OneInchV2 string
var exchangeName_ERC20 string
var exchangeName_orderBooks string
var mapOfExchangeNameToTopics = make(map[string][]common.Hash)

var topic_erc20Transfer string
var topic_erc20Allowance string

var flattenedValues []common.Hash
var numWorkers int
var allCurvePools []string
var err error

func init() {
	fmt.Println("nickdebug NewHeads: init() called - 222orderaggregator")
	numWorkers = runtime.NumCPU() - 1
	if numWorkers < 1 {
		numWorkers = 1 // Ensure at least one worker
	}
	// create a map of ExchangeName -> Topics
	//  those exchange names are copied from Ninja's codebase
	exchangeName_UniswapV2 = "UniswapV2"
	exchangeName_UniswapV3 = "UniswapV3"
	exchangeName_BalancerV2 = "BalancerV2"
	// var exchangeName_Curve string = "Curve"
	exchangeName_OneInchV2 = "OneInchV2"
	exchangeName_ERC20 = "ERC20"
	exchangeName_orderBooks = "orderBooks"

	topic_erc20Transfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	topic_erc20Allowance = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

	mapOfExchangeNameToTopics[exchangeName_UniswapV3] = []common.Hash{
		common.HexToHash("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"), // univ3 swap
		common.HexToHash("0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"), // univ3 mint
		common.HexToHash("0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"), // univ3 burn
	}
	mapOfExchangeNameToTopics[exchangeName_UniswapV2] = []common.Hash{
		common.HexToHash("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"), // uniswapV2 sync
	}
	//  TODO nick-smc check if we need both topics here
	mapOfExchangeNameToTopics[exchangeName_BalancerV2] = []common.Hash{
		common.HexToHash("0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b"), // balancerV2 swap
		common.HexToHash("0xe5ce249087ce04f05a957192435400fd97868dba0e6a4b4c049abf8af80dae78"), // balancerV2 poolBalancesChanged
	}
	var exchangeName_OneInchV2 string = "OneInchV2"
	mapOfExchangeNameToTopics[exchangeName_OneInchV2] = []common.Hash{
		common.HexToHash("0x8bab6aed5a508937051a144e61d6e61336834a66aaee250a00613ae6f744c422"), // OneInchV2 Deposited
		common.HexToHash("0x3cae9923fd3c2f468aa25a8ef687923e37f957459557c0380fd06526c0b8cdbc"), // OneInchV2 Withdrawn
		common.HexToHash("0xbd99c6719f088aa0abd9e7b7a4a635d1f931601e9f304b538dc42be25d8c65c6"), // OneInchV2 Swapped
	}
	var erc20Events = []common.Hash{
		common.HexToHash(topic_erc20Transfer),
		common.HexToHash(topic_erc20Allowance),
	}
	mapOfExchangeNameToTopics[exchangeName_ERC20] = erc20Events
	var orderbookEvents = []common.Hash{
		common.HexToHash("0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f"), // OtcOrderFilled
		common.HexToHash("0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124"), // LimitOrderFilled
		common.HexToHash("0xa6eb7cdc219e1518ced964e9a34e61d68a94e4f1569db3e84256ba981ba52753"), // OrderCancelled (0x and Tempo)
		common.HexToHash("0x2bf10746b5979a7ded837e52451fcc5341fe2485928bd737e11b16e1a29b9366"), // Tempo Fill
	}
	mapOfExchangeNameToTopics[exchangeName_orderBooks] = orderbookEvents
	// Flatten the map values
	for _, values := range mapOfExchangeNameToTopics {
		flattenedValues = append(flattenedValues, values...)
	}
	allCurvePools, err = GetAllPools_Curve()
	if err != nil {
		log.Error("nickdebug NewHeads: error getting allCurvePools: ", err)
	} else {
		fmt.Println("nickdebug NewHeads: allCurvePools", allCurvePools)
	}

	go startOrderBookAggregatorService()

}

type MyWaitGroup struct {
	wg      sync.WaitGroup
	counter int
	mu      sync.Mutex
}

func (mwg *MyWaitGroup) Add(delta int) {
	mwg.mu.Lock()
	defer mwg.mu.Unlock()
	mwg.counter += delta
	mwg.wg.Add(delta)
}

func (mwg *MyWaitGroup) Done() {
	mwg.mu.Lock()
	defer mwg.mu.Unlock()
	mwg.counter--
	mwg.wg.Done()
}

func (mwg *MyWaitGroup) Wait() {
	mwg.wg.Wait()
}

func (mwg *MyWaitGroup) Count() int {
	mwg.mu.Lock()
	defer mwg.mu.Unlock()
	return mwg.counter
}

// curveWorker processes Curve pools to fetch balance metadata.
// func curveWorker(id int, pools <-chan string, results chan<- PoolBalanceMetaData) {
func curveWorker(id int, pools <-chan string, results chan<- PoolBalanceMetaData, curveWg *MyWaitGroup) {
	for pool := range pools {
		// log.Info("curveWorker count (start)", "count", curveWg.Count(), "pool", pool)
		// Fetch balance metadata for the Curve pool
		balanceMetaData, err := getBalanceMetaData_Curve(pool)
		if err != nil {
			// Handle error, perhaps log it
			fmt.Printf("Worker %d: Error fetching balance metadata for Curve pool %s: %v\n", id, pool, err)
		}

		// Create PoolBalanceMetaData object
		poolAddress := common.HexToAddress(pool)
		poolBalanceMetaData := PoolBalanceMetaData{
			Address:         poolAddress,
			Topic:           common.Hash{}, // No topic for Curve in this example
			BalanceMetaData: balanceMetaData,
			ExchangeName:    "Curve",
		}

		// Send the result back
		results <- poolBalanceMetaData
		curveWg.Done()

		// log.Info("worker count (end)", "count", curveWg.Count(), "pool", pool)
	}
}

// Assuming that GetLogs returns []*types.Log
type Log = types.Log // Reusing the type from GetLogs
func logWorker(id int, logs <-chan *Log, results chan<- PoolBalanceMetaData, logWg *MyWaitGroup, currentBlockNumber *big.Int) {
	for eventLog := range logs {
		// log.Info("logWorker count (start)", "count", logWg.Count())

		address := eventLog.Address
		eventLogTopic := eventLog.Topics[0]
		balanceMetaData := interface{}(nil)
		var topicExchangeName string
		for exchangeName, topics := range mapOfExchangeNameToTopics {
			for _, topic := range topics {
				if topic == eventLogTopic {
					topicExchangeName = exchangeName
					break
				}
			}
		}

		var err error
		switch topicExchangeName {
		case exchangeName_ERC20:
			balanceMetaData, err = getBalanceMetaData_ERC20(address, eventLog)
		case exchangeName_UniswapV2:
			// log.Info("found UniswapV2 log...", "pool", address.Hex())
			balanceMetaData, err = getBalanceMetaData_UniswapV2(address.Hex())
			// log.Info("finished UniswapV2 log", "pool", address.Hex())
		case exchangeName_UniswapV3:
			// log.Info("found UniswapV3 log...", "pool", address.Hex())
			balanceMetaData, err = getBalanceMetaData_UniswapV3(address.Hex())
			// log.Info("finished UniswapV3 log", "pool", address.Hex())
		case exchangeName_BalancerV2:
			poolId := eventLog.Topics[1]
			// log.Info("found BalancerV2 log...", "poolId", poolId.Hex())
			balanceMetaData, address, err = getBalanceMetaData_BalancerV2(poolId)
			// log.Info("finished BalancerV2 log", "poolId", poolId.Hex())
		case exchangeName_OneInchV2:
			// log.Info("found OneInchV2 log...", "address", address.Hex())
			balanceMetaData, err = getBalanceMetaData_OneInchV2(address.Hex())
			AddPoolToActiveOneInchV2DecayPeriods(address, currentBlockNumber)
			// log.Info("finished OneInchV2 log", "address", address.Hex())
		case exchangeName_orderBooks:
			// log.Info("found ZrxOrderBook log...", "pool", address.Hex())
			balanceMetaData, err = getBalanceMetaData_OrderBooks(address, eventLog)
		default:
			// log.Error("NewHeads: unknown exchangeName", "topicExchangeName", topicExchangeName)
		}
		if err != nil {
			// there are NFTs and other bad contracts spamming errors. turn it back on for debugging
			// switch e := err.(type) {
			// case WrongFactoryAddressError:
			// 	log.Info("NewHeads: pool has wrong factory address", "topicExchangeName", topicExchangeName, "address:", e.Address)
			// default:
			// 	log.Error("NewHeads: error getting balanceMetaData: ", "topicExchangeName", topicExchangeName, "address:", address.Hex(), "error", err)
			// }
			balanceMetaData = interface{}(nil)
		}

		poolBalanceMetaData := PoolBalanceMetaData{
			Address:         address,
			Topic:           eventLogTopic,
			BalanceMetaData: balanceMetaData,
			ExchangeName:    topicExchangeName,
		}
		results <- poolBalanceMetaData
		logWg.Done()

		// log.Info("worker count (end)", "count", logWg.Count())
	}
}

func oneInchWorker(id int, pools <-chan common.Address, results chan<- PoolBalanceMetaData, oneInchWg *MyWaitGroup) {
	for poolAddress := range pools {
		// log.Info("oneInchWorker count (start)", "count", oneInchWg.Count())
		balanceMetaData := interface{}(nil)
		// Process the pool
		// Example: Fetching pool data (placeholder logic)
		balanceMetaData, err = getBalanceMetaData_OneInchV2(poolAddress.Hex())
		if err != nil {
			log.Error("NewHeads: error getting balanceMetaData on OneinchV2: ", "address:", poolAddress.Hex(), "error", err)
		}

		// Send the result back
		poolBalanceMetaData := PoolBalanceMetaData{
			Address:         poolAddress,
			Topic:           common.HexToHash("0xbd99c6719f088aa0abd9e7b7a4a635d1f931601e9f304b538dc42be25d8c65c6"),
			BalanceMetaData: balanceMetaData,
			ExchangeName:    exchangeName_OneInchV2,
		}
		results <- poolBalanceMetaData
		oneInchWg.Done()
		// log.Info("oneInchWorker count (end)", "count", oneInchWg.Count())
	}
}

// NewHeads send a notification each time a new (header) block is appended to the chain.
func (api *FilterAPI) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		// var logWg, curveWg sync.WaitGroup
		var logWg, curveWg MyWaitGroup
		headers := make(chan *types.Header)
		headersSub := api.events.SubscribeNewHeads(headers)
		defer headersSub.Unsubscribe()

		for {
			select {
			case h := <-headers:
				start := time.Now()
				// measure the time since h.Time
				hTime := time.Unix(int64(h.Time), 0)
				log.Info("NewHeads: block discovered", "block number", h.Number, "duration", time.Since(hTime))

				// log.Info("NewHeads: new block found", "block number", h.Number)
				blockHash := h.Hash()

				filterCriteria := FilterCriteria{
					BlockHash: &blockHash,
					FromBlock: nil,
					ToBlock:   nil,
					Addresses: nil,
					Topics:    [][]common.Hash{flattenedValues},
				}

				logs, err := api.GetLogs(ctx, filterCriteria)
				if err != nil {
					log.Error("NewHeads: error getting logs: ", err)
					continue
				}

				// Create channels for logs and results
				logChan := make(chan *types.Log, len(logs))          // Channel to send logs to logWorkers
				results := make(chan PoolBalanceMetaData, len(logs)) // Channel to collect results
				// log.Info("NewHeads: logChan and results built")
				// Start logWorkers
				logWg.Add(len(logs))
				for w := 1; w <= numWorkers; w++ {
					go func(id int) {
						logWorker(id, logChan, results, &logWg, h.Number)
					}(w)
				}
				// Send logs to the logChan channel
				for _, log := range logs {
					logChan <- log
				}

				// Collect results from logWorkers
				newHeadsWithPoolBalanceMetaData := NewHeadsWithPoolBalanceMetaData{
					Header:              h,
					PoolBalanceMetaData: make(map[common.Address]PoolBalanceMetaData),
				}
				for i := 0; i < len(logs); i++ {
					select {
					case result := <-results:
						if result.BalanceMetaData != nil {
							newHeadsWithPoolBalanceMetaData.PoolBalanceMetaData[result.Address] = result
						}
					case <-time.After(200 * time.Millisecond):
						log.Error("Timeout waiting for result from logWorker")
					}
				}

				// Create channels for Curve pools and results
				curvePoolsChan := make(chan string, len(allCurvePools))            // Channel to send Curve pools to curveWorkers
				curveResults := make(chan PoolBalanceMetaData, len(allCurvePools)) // Channel to collect results from curveWorkers
				// log.Info("NewHeads: curvePoolsChan and curveResults built")

				// Start Curve workers
				curveWg.Add(len(allCurvePools))
				for w := 1; w <= numWorkers; w++ {
					go func(id int) {
						curveWorker(id, curvePoolsChan, curveResults, &curveWg)
					}(w)
				}
				// Populate the curvePoolsChan with the global list of all Curve pools
				for _, pool := range allCurvePools {
					curvePoolsChan <- pool
				}
				for i := 0; i < len(allCurvePools); i++ {
					select {
					case result := <-curveResults:
						newHeadsWithPoolBalanceMetaData.PoolBalanceMetaData[result.Address] = result
					case <-time.After(200 * time.Millisecond): // 200 millisecond timeout
						log.Error("Timeout waiting for result from curveWorker")
					}
				}

				// // DEBUGGING START
				// // only execute this code every 50 blocks
				// oneInchPoolsToDebug, err := GetAllPools_OneInchV2()
				// if err != nil {
				// 	log.Error("NewHeads: error getting all OneInch pools: ", err)
				// }
				// for _, pool := range oneInchPoolsToDebug {
				// 	poolAddress := common.HexToAddress(pool)
				// 	AddPoolToActiveOneInchV2DecayPeriods(poolAddress, h.Number)
				// }
				// // DEBUGGING END

				// Create channels for OneInch decaying pools and results
				oneInchPools := GetAllDecayingOneinchPoolsData(h.Number) // Get pools that need to be queried
				// log.Info("NewHeads: oneInchPools", "count", len(oneInchPools))
				oneInchPoolsChan := make(chan common.Address, len(oneInchPools))    // Channel to send OneInch pools to workers
				oneInchResults := make(chan PoolBalanceMetaData, len(oneInchPools)) // Channel to collect results from OneInch workers)

				// Start OneInch workers
				var oneInchWg MyWaitGroup
				oneInchWg.Add(len(oneInchPools))
				for w := 1; w <= numWorkers; w++ { // numOneInchWorkers is the number of worker goroutines you want to start
					go oneInchWorker(w, oneInchPoolsChan, oneInchResults, &oneInchWg)
				}

				// Populate the oneInchPoolsChan with pools
				for _, pool := range oneInchPools {
					oneInchPoolsChan <- pool.PoolAddress
				}

				// Collect results from OneInch workers
				for i := 0; i < len(oneInchPools); i++ {
					select {
					case result := <-oneInchResults:
						newHeadsWithPoolBalanceMetaData.PoolBalanceMetaData[result.Address] = result
					case <-time.After(200 * time.Millisecond): // Timeout waiting for worker
						log.Error("Timeout waiting for result from oneInchWorker")
					}
				}

				// Wait for all workers to finish and then close the channels
				// log.Info("NewHeads: waiting for log workers to finish...", "logWg.Count()", logWg.Count())
				logWg.Wait()
				// log.Info("NewHeads: waiting for curve workers to finish...", "curveWg.Count()", curveWg.Count())
				curveWg.Wait()
				// log.Info("NewHeads: waiting for oneInch workers to finish...", "oneInchWg.Count()", oneInchWg.Count())
				oneInchWg.Wait()

				close(logChan)
				close(curvePoolsChan)
				// Wait for OneInch workers to finish and then close the channels
				close(oneInchPoolsChan)
				// log.Info("NewHeads: all channels closed")

				notifier.Notify(rpcSub.ID, newHeadsWithPoolBalanceMetaData)
				// get the timestamp of the block
				log.Info("NewHeads: time to process logs and notify", "duration", time.Since(start))

			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// Logs creates a subscription that fires for all new log that match the given filter criteria.
func (api *FilterAPI) Logs(ctx context.Context, crit FilterCriteria) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	var (
		rpcSub      = notifier.CreateSubscription()
		matchedLogs = make(chan []*types.Log)
	)

	logsSub, err := api.events.SubscribeLogs(ethereum.FilterQuery(crit), matchedLogs)
	if err != nil {
		return nil, err
	}

	go func() {
		defer logsSub.Unsubscribe()
		for {
			select {
			case logs := <-matchedLogs:
				for _, log := range logs {
					log := log
					notifier.Notify(rpcSub.ID, &log)
				}
			case <-rpcSub.Err(): // client send an unsubscribe request
				return
			}
		}
	}()

	return rpcSub, nil
}

// FilterCriteria represents a request to create a new filter.
// Same as ethereum.FilterQuery but with UnmarshalJSON() method.
type FilterCriteria ethereum.FilterQuery

// NewFilter creates a new filter and returns the filter id. It can be
// used to retrieve logs when the state changes. This method cannot be
// used to fetch logs that are already stored in the state.
//
// Default criteria for the from and to block are "latest".
// Using "latest" as block number will return logs for mined blocks.
// Using "pending" as block number returns logs for not yet mined (pending) blocks.
// In case logs are removed (chain reorg) previously returned logs are returned
// again but with the removed property set to true.
//
// In case "fromBlock" > "toBlock" an error is returned.
func (api *FilterAPI) NewFilter(crit FilterCriteria) (rpc.ID, error) {
	logs := make(chan []*types.Log)
	logsSub, err := api.events.SubscribeLogs(ethereum.FilterQuery(crit), logs)
	if err != nil {
		return "", err
	}

	api.filtersMu.Lock()
	api.filters[logsSub.ID] = &filter{typ: LogsSubscription, crit: crit, deadline: time.NewTimer(api.timeout), logs: make([]*types.Log, 0), s: logsSub}
	api.filtersMu.Unlock()

	go func() {
		for {
			select {
			case l := <-logs:
				api.filtersMu.Lock()
				if f, found := api.filters[logsSub.ID]; found {
					f.logs = append(f.logs, l...)
				}
				api.filtersMu.Unlock()
			case <-logsSub.Err():
				api.filtersMu.Lock()
				delete(api.filters, logsSub.ID)
				api.filtersMu.Unlock()
				return
			}
		}
	}()

	return logsSub.ID, nil
}

// GetLogs returns logs matching the given argument that are stored within the state.
func (api *FilterAPI) GetLogs(ctx context.Context, crit FilterCriteria) ([]*types.Log, error) {
	if len(crit.Topics) > maxTopics {
		return nil, errExceedMaxTopics
	}
	var filter *Filter
	if crit.BlockHash != nil {
		// Block filter requested, construct a single-shot filter
		filter = api.sys.NewBlockFilter(*crit.BlockHash, crit.Addresses, crit.Topics)
	} else {
		// Convert the RPC block numbers into internal representations
		begin := rpc.LatestBlockNumber.Int64()
		if crit.FromBlock != nil {
			begin = crit.FromBlock.Int64()
		}
		end := rpc.LatestBlockNumber.Int64()
		if crit.ToBlock != nil {
			end = crit.ToBlock.Int64()
		}
		if begin > 0 && end > 0 && begin > end {
			return nil, errInvalidBlockRange
		}
		// Construct the range filter
		filter = api.sys.NewRangeFilter(begin, end, crit.Addresses, crit.Topics)
	}
	// Run the filter and return all the logs
	logs, err := filter.Logs(ctx)
	if err != nil {
		return nil, err
	}
	return returnLogs(logs), err
}

// UninstallFilter removes the filter with the given filter id.
func (api *FilterAPI) UninstallFilter(id rpc.ID) bool {
	api.filtersMu.Lock()
	f, found := api.filters[id]
	if found {
		delete(api.filters, id)
	}
	api.filtersMu.Unlock()
	if found {
		f.s.Unsubscribe()
	}

	return found
}

// GetFilterLogs returns the logs for the filter with the given id.
// If the filter could not be found an empty array of logs is returned.
func (api *FilterAPI) GetFilterLogs(ctx context.Context, id rpc.ID) ([]*types.Log, error) {
	api.filtersMu.Lock()
	f, found := api.filters[id]
	api.filtersMu.Unlock()

	if !found || f.typ != LogsSubscription {
		return nil, errFilterNotFound
	}

	var filter *Filter
	if f.crit.BlockHash != nil {
		// Block filter requested, construct a single-shot filter
		filter = api.sys.NewBlockFilter(*f.crit.BlockHash, f.crit.Addresses, f.crit.Topics)
	} else {
		// Convert the RPC block numbers into internal representations
		begin := rpc.LatestBlockNumber.Int64()
		if f.crit.FromBlock != nil {
			begin = f.crit.FromBlock.Int64()
		}
		end := rpc.LatestBlockNumber.Int64()
		if f.crit.ToBlock != nil {
			end = f.crit.ToBlock.Int64()
		}
		// Construct the range filter
		filter = api.sys.NewRangeFilter(begin, end, f.crit.Addresses, f.crit.Topics)
	}
	// Run the filter and return all the logs
	logs, err := filter.Logs(ctx)
	if err != nil {
		return nil, err
	}
	return returnLogs(logs), nil
}

// GetFilterChanges returns the logs for the filter with the given id since
// last time it was called. This can be used for polling.
//
// For pending transaction and block filters the result is []common.Hash.
// (pending)Log filters return []Log.
func (api *FilterAPI) GetFilterChanges(id rpc.ID) (interface{}, error) {
	api.filtersMu.Lock()
	defer api.filtersMu.Unlock()

	chainConfig := api.sys.backend.ChainConfig()
	latest := api.sys.backend.CurrentHeader()

	if f, found := api.filters[id]; found {
		if !f.deadline.Stop() {
			// timer expired but filter is not yet removed in timeout loop
			// receive timer value and reset timer
			<-f.deadline.C
		}
		f.deadline.Reset(api.timeout)

		switch f.typ {
		case BlocksSubscription:
			hashes := f.hashes
			f.hashes = nil
			return returnHashes(hashes), nil
		case PendingTransactionsSubscription:
			if f.fullTx {
				txs := make([]*ethapi.RPCTransaction, 0, len(f.txs))
				for _, tx := range f.txs {
					txs = append(txs, ethapi.NewRPCPendingTransaction(tx, latest, chainConfig))
				}
				f.txs = nil
				return txs, nil
			} else {
				hashes := make([]common.Hash, 0, len(f.txs))
				for _, tx := range f.txs {
					hashes = append(hashes, tx.Hash())
				}
				f.txs = nil
				return hashes, nil
			}
		case LogsSubscription:
			logs := f.logs
			f.logs = nil
			return returnLogs(logs), nil
		}
	}

	return []interface{}{}, errFilterNotFound
}

// returnHashes is a helper that will return an empty hash array case the given hash array is nil,
// otherwise the given hashes array is returned.
func returnHashes(hashes []common.Hash) []common.Hash {
	if hashes == nil {
		return []common.Hash{}
	}
	return hashes
}

// returnLogs is a helper that will return an empty log array in case the given logs array is nil,
// otherwise the given logs array is returned.
func returnLogs(logs []*types.Log) []*types.Log {
	if logs == nil {
		return []*types.Log{}
	}
	return logs
}

// UnmarshalJSON sets *args fields with given data.
func (args *FilterCriteria) UnmarshalJSON(data []byte) error {
	type input struct {
		BlockHash *common.Hash     `json:"blockHash"`
		FromBlock *rpc.BlockNumber `json:"fromBlock"`
		ToBlock   *rpc.BlockNumber `json:"toBlock"`
		Addresses interface{}      `json:"address"`
		Topics    []interface{}    `json:"topics"`
	}

	var raw input
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if raw.BlockHash != nil {
		if raw.FromBlock != nil || raw.ToBlock != nil {
			// BlockHash is mutually exclusive with FromBlock/ToBlock criteria
			return errors.New("cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other")
		}
		args.BlockHash = raw.BlockHash
	} else {
		if raw.FromBlock != nil {
			args.FromBlock = big.NewInt(raw.FromBlock.Int64())
		}

		if raw.ToBlock != nil {
			args.ToBlock = big.NewInt(raw.ToBlock.Int64())
		}
	}

	args.Addresses = []common.Address{}

	if raw.Addresses != nil {
		// raw.Address can contain a single address or an array of addresses
		switch rawAddr := raw.Addresses.(type) {
		case []interface{}:
			for i, addr := range rawAddr {
				if strAddr, ok := addr.(string); ok {
					addr, err := decodeAddress(strAddr)
					if err != nil {
						return fmt.Errorf("invalid address at index %d: %v", i, err)
					}
					args.Addresses = append(args.Addresses, addr)
				} else {
					return fmt.Errorf("non-string address at index %d", i)
				}
			}
		case string:
			addr, err := decodeAddress(rawAddr)
			if err != nil {
				return fmt.Errorf("invalid address: %v", err)
			}
			args.Addresses = []common.Address{addr}
		default:
			return errors.New("invalid addresses in query")
		}
	}
	if len(raw.Topics) > maxTopics {
		return errExceedMaxTopics
	}

	// topics is an array consisting of strings and/or arrays of strings.
	// JSON null values are converted to common.Hash{} and ignored by the filter manager.
	if len(raw.Topics) > 0 {
		args.Topics = make([][]common.Hash, len(raw.Topics))
		for i, t := range raw.Topics {
			switch topic := t.(type) {
			case nil:
				// ignore topic when matching logs

			case string:
				// match specific topic
				top, err := decodeTopic(topic)
				if err != nil {
					return err
				}
				args.Topics[i] = []common.Hash{top}

			case []interface{}:
				// or case e.g. [null, "topic0", "topic1"]
				if len(topic) > maxSubTopics {
					return errExceedMaxTopics
				}
				for _, rawTopic := range topic {
					if rawTopic == nil {
						// null component, match all
						args.Topics[i] = nil
						break
					}
					if topic, ok := rawTopic.(string); ok {
						parsed, err := decodeTopic(topic)
						if err != nil {
							return err
						}
						args.Topics[i] = append(args.Topics[i], parsed)
					} else {
						return errInvalidTopic
					}
				}
			default:
				return errInvalidTopic
			}
		}
	}

	return nil
}

func decodeAddress(s string) (common.Address, error) {
	b, err := hexutil.Decode(s)
	if err == nil && len(b) != common.AddressLength {
		err = fmt.Errorf("hex has invalid length %d after decoding; expected %d for address", len(b), common.AddressLength)
	}
	return common.BytesToAddress(b), err
}

func decodeTopic(s string) (common.Hash, error) {
	b, err := hexutil.Decode(s)
	if err == nil && len(b) != common.HashLength {
		err = fmt.Errorf("hex has invalid length %d after decoding; expected %d for topic", len(b), common.HashLength)
	}
	return common.BytesToHash(b), err
}
