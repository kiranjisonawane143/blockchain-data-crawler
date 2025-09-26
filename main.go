package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "math/big"
    "os"
    "time"

    "github.com/ethereum/go-ethereum"
    "github.com/ethereum/go-ethereum/accounts/abi"
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/ethclient"
    _ "github.com/lib/pq"
)

type BlockchainCrawler struct {
    client       *ethclient.Client
    db          *sql.DB
    startBlock  uint64
    endBlock    uint64
    batchSize   uint64
    contracts   map[string]ContractConfig
}

type ContractConfig struct {
    Address string                 `json:"address"`
    ABI     string                 `json:"abi"`
    Events  map[string]EventConfig `json:"events"`
}

type EventConfig struct {
    Signature string            `json:"signature"`
    Fields    map[string]string `json:"fields"`
}

type TransactionData struct {
    Hash        string    `json:"hash"`
    From        string    `json:"from"`
    To          string    `json:"to"`
    Value       string    `json:"value"`
    GasPrice    string    `json:"gas_price"`
    GasUsed     uint64    `json:"gas_used"`
    BlockNumber uint64    `json:"block_number"`
    Timestamp   time.Time `json:"timestamp"`
    Status      uint64    `json:"status"`
    Input       string    `json:"input"`
}

type EventData struct {
    TxHash      string                 `json:"tx_hash"`
    LogIndex    uint     `json:"log_index"`
    Address     string                 `json:"address"`
    EventName   string                 `json:"event_name"`
    BlockNumber uint64                 `json:"block_number"`
    Timestamp   time.Time              `json:"timestamp"`
    Data        map[string]interface{} `json:"data"`
}

type TokenTransfer struct {
    TxHash      string    `json:"tx_hash"`
    LogIndex    uint      `json:"log_index"`
    From        string    `json:"from"`
    To          string    `json:"to"`
    Value       string    `json:"value"`
    Token       string    `json:"token"`
    BlockNumber uint64    `json:"block_number"`
    Timestamp   time.Time `json:"timestamp"`
}

func NewBlockchainCrawler(rpcURL, dbURL string, startBlock, endBlock, batchSize uint64) (*BlockchainCrawler, error) {
    client, err := ethclient.Dial(rpcURL)
    if err != nil {
        return nil, err
    }

    db, err := sql.Open("postgres", dbURL)
    if err != nil {
        return nil, err
    }

    if err := db.Ping(); err != nil {
        return nil, err
    }

    crawler := &BlockchainCrawler{
        client:    client,
        db:        db,
        startBlock: startBlock,
        endBlock:  endBlock,
        batchSize: batchSize,
        contracts: make(map[string]ContractConfig),
    }

    if err := crawler.initDatabase(); err != nil {
        return nil, err
    }

    return crawler, nil
}

func (bc *BlockchainCrawler) initDatabase() error {
    queries := []string{
        `CREATE TABLE IF NOT EXISTS blocks (
            number BIGINT PRIMARY KEY,
            hash VARCHAR(66) UNIQUE NOT NULL,
            parent_hash VARCHAR(66) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            gas_limit BIGINT NOT NULL,
            gas_used BIGINT NOT NULL,
            miner VARCHAR(42) NOT NULL,
            difficulty VARCHAR(100) NOT NULL,
            total_difficulty VARCHAR(100) NOT NULL,
            size_bytes BIGINT NOT NULL,
            tx_count INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )`,
        
        `CREATE TABLE IF NOT EXISTS transactions (
            hash VARCHAR(66) PRIMARY KEY,
            from_address VARCHAR(42) NOT NULL,
            to_address VARCHAR(42),
            value VARCHAR(100) NOT NULL,
            gas_price VARCHAR(100) NOT NULL,
            gas_used BIGINT NOT NULL,
            block_number BIGINT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            status INTEGER NOT NULL,
            input_data TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            INDEX(block_number),
            INDEX(from_address),
            INDEX(to_address),
            INDEX(timestamp)
        )`,
        
        `CREATE TABLE IF NOT EXISTS token_transfers (
            id SERIAL PRIMARY KEY,
            tx_hash VARCHAR(66) NOT NULL,
            log_index INTEGER NOT NULL,
            from_address VARCHAR(42) NOT NULL,
            to_address VARCHAR(42) NOT NULL,
            value VARCHAR(100) NOT NULL,
            token_address VARCHAR(42) NOT NULL,
            block_number BIGINT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(tx_hash, log_index),
            INDEX(token_address),
            INDEX(from_address),
            INDEX(to_address),
            INDEX(block_number)
        )`,
        
        `CREATE TABLE IF NOT EXISTS contract_events (
            id SERIAL PRIMARY KEY,
            tx_hash VARCHAR(66) NOT NULL,
            log_index INTEGER NOT NULL,
            contract_address VARCHAR(42) NOT NULL,
            event_name VARCHAR(100) NOT NULL,
            block_number BIGINT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            event_data JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(tx_hash, log_index),
            INDEX(contract_address),
            INDEX(event_name),
            INDEX(block_number)
        )`,
    }

    for _, query := range queries {
        if _, err := bc.db.Exec(query); err != nil {
            return fmt.Errorf("failed to create table: %w", err)
        }
    }

    return nil
}

func (bc *BlockchainCrawler) LoadContractConfig(configFile string) error {
    data, err := os.ReadFile(configFile)
    if err != nil {
        return err
    }

    return json.Unmarshal(data, &bc.contracts)
}

func (bc *BlockchainCrawler) Start() error {
    log.Printf("开始爬取区块 %d 到 %d", bc.startBlock, bc.endBlock)

    for blockNum := bc.startBlock; blockNum <= bc.endBlock; blockNum += bc.batchSize {
        endBlock := blockNum + bc.batchSize - 1
        if endBlock > bc.endBlock {
            endBlock = bc.endBlock
        }

        log.Printf("处理区块范围: %d - %d", blockNum, endBlock)

        if err := bc.processBatch(blockNum, endBlock); err != nil {
            log.Printf("处理批次失败: %v", err)
            continue
        }

        // 避免过度请求
        time.Sleep(time.Millisecond * 100)
    }

    log.Println("爬取完成")
    return nil
}

func (bc *BlockchainCrawler) processBatch(startBlock, endBlock uint64) error {
    for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
        if err := bc.processBlock(blockNum); err != nil {
            return fmt.Errorf("处理区块 %d 失败: %w", blockNum, err)
        }
    }
    return nil
}

func (bc *BlockchainCrawler) processBlock(blockNumber uint64) error {
    ctx := context.Background()
    
    block, err := bc.client.BlockByNumber(ctx, big.NewInt(int64(blockNumber)))
    if err != nil {
        return err
    }

    // 保存区块信息
    if err := bc.saveBlock(block); err != nil {
        return err
    }

    // 处理所有交易
    for _, tx := range block.Transactions() {
        if err := bc.processTransaction(tx, block); err != nil {
            log.Printf("处理交易 %s 失败: %v", tx.Hash().Hex(), err)
            continue
        }
    }

    // 处理合约事件
    if err := bc.processContractEvents(block); err != nil {
        log.Printf("处理合约事件失败: %v", err)
    }

    log.Printf("完成处理区块 %d，包含 %d 笔交易", blockNumber, len(block.Transactions()))
    return nil
}

func (bc *BlockchainCrawler) saveBlock(block *types.Block) error {
    query := `
        INSERT INTO blocks (
            number, hash, parent_hash, timestamp, gas_limit, gas_used, 
            miner, difficulty, total_difficulty, size_bytes, tx_count
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (number) DO NOTHING`

    timestamp := time.Unix(int64(block.Time()), 0)

    _, err := bc.db.Exec(query,
        block.Number().Uint64(),
        block.Hash().Hex(),
        block.ParentHash().Hex(),
        timestamp,
        block.GasLimit(),
        block.GasUsed(),
        block.Coinbase().Hex(),
        block.Difficulty().String(),
        "0", // total_difficulty需要单独获取
        block.Size(),
        len(block.Transactions()),
    )

    return err
}

func (bc *BlockchainCrawler) processTransaction(tx *types.Transaction, block *types.Block) error {
    ctx := context.Background()
    
    // 获取交易收据
    receipt, err := bc.client.TransactionReceipt(ctx, tx.Hash())
    if err != nil {
        return err
    }

    // 保存交易基本信息
    txData := TransactionData{
        Hash:        tx.Hash().Hex(),
        From:        bc.getTransactionSender(tx),
        Value:       tx.Value().String(),
        GasPrice:    tx.GasPrice().String(),
        GasUsed:     receipt.GasUsed,
        BlockNumber: block.Number().Uint64(),
        Timestamp:   time.Unix(int64(block.Time()), 0),
        Status:      receipt.Status,
        Input:       fmt.Sprintf("0x%x", tx.Data()),
    }

    if tx.To() != nil {
        txData.To = tx.To().Hex()
    }

    if err := bc.saveTransaction(txData); err != nil {
        return err
    }

    // 处理ERC20转账事件
    if err := bc.processTokenTransfers(receipt, block); err != nil {
        log.Printf("处理代币转账失败: %v", err)
    }

    return nil
}

func (bc *BlockchainCrawler) getTransactionSender(tx *types.Transaction) string {
    from, err := types.Sender(types.NewEIP155Signer(tx.ChainId()), tx)
    if err != nil {
        return ""
    }
    return from.Hex()
}

func (bc *BlockchainCrawler) saveTransaction(tx TransactionData) error {
    query := `
        INSERT INTO transactions (
            hash, from_address, to_address, value, gas_price, gas_used,
            block_number, timestamp, status, input_data
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (hash) DO NOTHING`

    _, err := bc.db.Exec(query,
        tx.Hash, tx.From, tx.To, tx.Value, tx.GasPrice, tx.GasUsed,
        tx.BlockNumber, tx.Timestamp, tx.Status, tx.Input,
    )

    return err
}

func (bc *BlockchainCrawler) processTokenTransfers(receipt *types.Receipt, block *types.Block) error {
    // ERC20 Transfer event signature
    transferTopic := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

    for _, vLog := range receipt.Logs {
        if len(vLog.Topics) > 0 && vLog.Topics[0] == transferTopic {
            if len(vLog.Topics) == 3 && len(vLog.Data) == 32 {
                transfer := TokenTransfer{
                    TxHash:      receipt.TxHash.Hex(),
                    LogIndex:    vLog.Index,
                    From:        common.HexToAddress(vLog.Topics[1].Hex()).Hex(),
                    To:          common.HexToAddress(vLog.Topics[2].Hex()).Hex(),
                    Value:       new(big.Int).SetBytes(vLog.Data).String(),
                    Token:       vLog.Address.Hex(),
                    BlockNumber: block.Number().Uint64(),
                    Timestamp:   time.Unix(int64(block.Time()), 0),
                }

                if err := bc.saveTokenTransfer(transfer); err != nil {
                    log.Printf("保存代币转账失败: %v", err)
                }
            }
        }
    }

    return nil
}

func (bc *BlockchainCrawler) saveTokenTransfer(transfer TokenTransfer) error {
    query := `
        INSERT INTO token_transfers (
            tx_hash, log_index, from_address, to_address, value,
            token_address, block_number, timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (tx_hash, log_index) DO NOTHING`

    _, err := bc.db.Exec(query,
        transfer.TxHash, transfer.LogIndex, transfer.From, transfer.To,
        transfer.Value, transfer.Token, transfer.BlockNumber, transfer.Timestamp,
    )

    return err
}

func (bc *BlockchainCrawler) processContractEvents(block *types.Block) error {
    // 处理配置中指定的合约事件
    for contractAddr, config := range bc.contracts {
        if err := bc.processContractEventsForAddress(contractAddr, config, block); err != nil {
            return err
        }
    }

    return nil
}

func (bc *BlockchainCrawler) processContractEventsForAddress(contractAddr string, config ContractConfig, block *types.Block) error {
    ctx := context.Background()

    // 构建事件过滤器
    query := ethereum.FilterQuery{
        FromBlock: block.Number(),
        ToBlock:   block.Number(),
        Addresses: []common.Address{common.HexToAddress(contractAddr)},
    }

    logs, err := bc.client.FilterLogs(ctx, query)
    if err != nil {
        return err
    }

    // 解析ABI
    contractABI, err := abi.JSON(strings.NewReader(config.ABI))
    if err != nil {
        return err
    }

    for _, vLog := range logs {
        // 解析事件
        for eventName, eventConfig := range config.Events {
            if len(vLog.Topics) > 0 && vLog.Topics[0].Hex() == eventConfig.Signature {
                eventData := make(map[string]interface{})

                // 解析事件参数
                if err := contractABI.UnpackIntoMap(eventData, eventName, vLog.Data); err != nil {
                    log.Printf("解析事件数据失败: %v", err)
                    continue
                }

                // 添加indexed参数
                for i, topic := range vLog.Topics[1:] {
                    if i < len(eventConfig.Fields) {
                        // 这里需要根据具体的字段类型进行解析
                        eventData[fmt.Sprintf("indexed_%d", i)] = topic.Hex()
                    }
                }

                event := EventData{
                    TxHash:      vLog.TxHash.Hex(),
                    LogIndex:    vLog.Index,
                    Address:     vLog.Address.Hex(),
                    EventName:   eventName,
                    BlockNumber: block.Number().Uint64(),
                    Timestamp:   time.Unix(int64(block.Time()), 0),
                    Data:        eventData,
                }

                if err := bc.saveContractEvent(event); err != nil {
                    log.Printf("保存合约事件失败: %v", err)
                }
            }
        }
    }

    return nil
}

func (bc *BlockchainCrawler) saveContractEvent(event EventData) error {
    eventDataJSON, err := json.Marshal(event.Data)
    if err != nil {
        return err
    }

    query := `
        INSERT INTO contract_events (
            tx_hash, log_index, contract_address, event_name,
            block_number, timestamp, event_data
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (tx_hash, log_index) DO NOTHING`

    _, err = bc.db.Exec(query,
        event.TxHash, event.LogIndex, event.Address, event.EventName,
        event.BlockNumber, event.Timestamp, string(eventDataJSON),
    )

    return err
}

// 分析工具
func (bc *BlockchainCrawler) GetTopTokensByVolume(limit int, days int) ([]TokenStats, error) {
    query := `
        SELECT 
            token_address,
            COUNT(*) as transfer_count,
            COUNT(DISTINCT from_address) as unique_senders,
            COUNT(DISTINCT to_address) as unique_receivers,
            SUM(CAST(value AS DECIMAL)) as total_volume
        FROM token_transfers 
        WHERE timestamp > NOW() - INTERVAL '%d days'
        GROUP BY token_address
        ORDER BY total_volume DESC
        LIMIT $1`

    rows, err := bc.db.Query(fmt.Sprintf(query, days), limit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var stats []TokenStats
    for rows.Next() {
        var stat TokenStats
        if err := rows.Scan(&stat.TokenAddress, &stat.TransferCount, 
            &stat.UniqueSenders, &stat.UniqueReceivers, &stat.TotalVolume); err != nil {
            return nil, err
        }
        stats = append(stats, stat)
    }

    return stats, nil
}

type TokenStats struct {
    TokenAddress    string `json:"token_address"`
    TransferCount   int    `json:"transfer_count"`
    UniqueSenders   int    `json:"unique_senders"`
    UniqueReceivers int    `json:"unique_receivers"`
    TotalVolume     string `json:"total_volume"`
}

func main() {
    rpcURL := "https://mainnet.infura.io/v3/YOUR_INFURA_KEY"
    dbURL := "postgres://user:password@localhost/blockchain_data?sslmode=disable"
    
    crawler, err := NewBlockchainCrawler(rpcURL, dbURL, 18800000, 18800100, 10)
    if err != nil {
        log.Fatal(err)
    }
    defer crawler.db.Close()

    // 加载合约配置（可选）
    // if err := crawler.LoadContractConfig("contracts.json"); err != nil {
    //     log.Printf("加载合约配置失败: %v", err)
    // }

    if err := crawler.Start(); err != nil {
        log.Fatal(err)
    }

    // 分析示例
    stats, err := crawler.GetTopTokensByVolume(10, 7)
    if err != nil {
        log.Printf("获取代币统计失败: %v", err)
    } else {
        fmt.Println("Top tokens by volume:")
        for _, stat := range stats {
            fmt.Printf("Token: %s, Transfers: %d, Volume: %s\n", 
                stat.TokenAddress, stat.TransferCount, stat.TotalVolume)
        }
    }
}
