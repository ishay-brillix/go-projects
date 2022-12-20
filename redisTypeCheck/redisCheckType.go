package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

func main() {
	fmt.Println("Starting...")
	// Initialize variables
	pattern := flag.String("pattern", "", "pattern to use in SCAN, default empty string")
	batchSize := flag.Int("batchSize", 10, "The size of the batch to use in the SCAN, default 10")
	separator := flag.String("separator", ":", "separator used in key names, default :")
	flag.Parse()

	var cursor uint64
	keyspace := make(map[string]int)
	dataTypes := make(map[string]int)
	var keys []string
	var err error
	var prog int

	// Create redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	keys, cursor, err = rdb.Scan(ctx, cursor, *pattern, int64(*batchSize)).Result()
	check(err)

	if len(keys) == 0 {
		fmt.Println("No keys were found")
	} else {
		processKeys(&keys, keyspace, dataTypes, rdb, *separator)
		checkProg(&prog, len(keys))
		for cursor > 0 {
			keys, cursor, err = rdb.Scan(ctx, cursor, *pattern, int64(*batchSize)).Result()
			check(err)
			processKeys(&keys, keyspace, dataTypes, rdb, *separator)
			checkProg(&prog, len(keys))
		}
	}
	keyspaceJson, _ := json.Marshal(keyspace)
	dataTypesJson, _ := json.Marshal(dataTypes)
	err = os.WriteFile("detailed_keyspace.json", keyspaceJson, 0644)
	check(err)
	err = os.WriteFile("data_types_count.json", dataTypesJson, 0644)
	check(err)
	fmt.Println("Total keys processed:", prog)
	// wg.Done()
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func checkProg(prog *int, lastBatchSize int) {
	currProg := *prog
	*prog = *prog + lastBatchSize
	if *prog-currProg >= 1000 {
		fmt.Println("Processed keys:", *prog)
	}
}

func processKey(key string, separator string, rdb *redis.Client) (string, string) {
	keyType := *rdb.Type(ctx, key)
	return strings.Split(key, separator)[0] + ":" + keyType.Val(), keyType.Val()
}

func processKeys(keys *[]string, keyspace map[string]int, dataTypes map[string]int, rdb *redis.Client, separator string) {
	for _, value := range *keys {
		keyNameType, keyType := processKey(value, separator, rdb)
		keyspace[keyNameType]++
		dataTypes[keyType]++
	}
}
