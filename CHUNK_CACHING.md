# Chunk Caching Feature

This feature provides local caching of result set chunks to solve the issue where chunk URLs expire after approximately 6 hours, causing errors when accessing large result sets over extended periods.

## Problem Solved

When Snowflake returns large result sets, it splits them into chunks stored in cloud storage (S3, Azure, GCS) with signed URLs that expire. This causes errors like:

```
262000 (08006): failed to get a chunk of result sets. idx: 36
```

The chunk caching feature downloads and stores chunks locally, allowing you to access result data even after the original URLs expire.

## Usage

### Basic Example

```go
package main

import (
    "context"
    "database/sql"
    "time"
    
    "github.com/snowflakedb/gosnowflake"
)

func main() {
    // Configure chunk caching
    cacheConfig := gosnowflake.ChunkCacheConfig{
        CacheDir: "/tmp/snowflake_chunks", // Directory to store cached chunks
        MaxAge:   24 * time.Hour,         // Cache chunks for 24 hours
        Enabled:  true,                   // Enable caching
    }

    // Create context with caching enabled
    ctx := gosnowflake.WithChunkCaching(context.Background(), cacheConfig)

    // Open database connection
    db, err := sql.Open("snowflake", dsn)
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Execute query with caching
    rows, err := db.QueryContext(ctx, "SELECT * FROM large_table")
    if err != nil {
        panic(err)
    }
    defer rows.Close()

    // Process results - chunks will be cached automatically
    for rows.Next() {
        // Your row processing logic
    }
}
```

### Configuration Options

```go
type ChunkCacheConfig struct {
    // CacheDir is the directory where chunks will be cached
    CacheDir string
    
    // MaxAge is the maximum age of cached chunks before they're considered stale
    MaxAge time.Duration
    
    // Enabled controls whether caching is active
    Enabled bool
}
```

### Cache Management

Clean up cached chunks:

```go
config := gosnowflake.ChunkCacheConfig{
    CacheDir: "/tmp/snowflake_chunks",
    MaxAge:   24 * time.Hour,
    Enabled:  true,
}

// Option 1: Remove chunks older than MaxAge
err := gosnowflake.CleanChunkCache(config)
if err != nil {
    log.Printf("Failed to clean old cache: %v", err)
}

// Option 2: Remove ALL cached chunks (typically after processing is complete)
err = gosnowflake.ClearAllChunkCache(config)
if err != nil {
    log.Printf("Failed to clear all cache: %v", err)
}
```

## How It Works

1. **Immediate Download**: When you execute a query with caching enabled, ALL chunks are downloaded and cached to disk immediately during query initialization, before any URLs expire.

2. **Transparent Integration**: The caching is implemented by extending the existing chunk downloader's `start()` method and replacing the download helper function.

3. **Cache Key Generation**: Each chunk is cached using a unique key generated from the chunk URL and encryption key (QRMK).

4. **Cache Validation**: Before downloading, the system checks if valid cached versions already exist based on file modification time and the configured `MaxAge`.

5. **Atomic Operations**: Chunks are downloaded to temporary files and atomically moved to prevent corruption from incomplete downloads.

6. **On-Demand Decoding**: While chunks are downloaded immediately, they are decoded on-demand as you iterate through the result set, keeping memory usage reasonable.

7. **Fallback Behavior**: If chunk caching fails, the system automatically falls back to the original on-demand download behavior.

## Benefits

- **Eliminates URL Expiration Errors**: Access result data hours after the original query without URL expiration issues
- **Improved Performance**: Subsequent access to the same data is much faster
- **Transparent**: No changes needed to existing query code
- **Configurable**: Fine-tune cache behavior for your specific needs
- **Safe**: Disabled by default, explicit opt-in required

## Cache Storage

- Chunks are stored as individual files in the configured cache directory
- File names are MD5 hashes of the chunk URL and encryption key
- Files are created with `.tmp` extension and atomically renamed to prevent corruption
- Cache cleaning removes files older than the configured `MaxAge`

## Thread Safety

The caching implementation is thread-safe and works correctly with the existing chunk download concurrency mechanisms in gosnowflake.

## Performance Considerations

- **Disk Space**: Cached chunks consume local disk space. Monitor usage and clean periodically.
- **Initial Download**: First access still requires downloading all chunks
- **Cache Hits**: Subsequent access is limited by local disk I/O rather than network

## Security Notes

- Cached chunks contain your actual data in unencrypted form on local disk
- Ensure appropriate file system permissions on the cache directory
- Consider encrypting the cache directory if storing sensitive data
- Regular cleanup prevents indefinite accumulation of cached data