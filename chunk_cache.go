package gosnowflake

import (
	"bufio"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// ChunkCacheConfig configures local chunk caching behavior
type ChunkCacheConfig struct {
	// CacheDir is the directory where chunks will be cached
	CacheDir string
	// MaxAge is the maximum age of cached chunks before they're considered stale
	MaxAge time.Duration
	// Enabled controls whether caching is active
	Enabled bool
}

// getCacheKey generates a cache key from chunk URL and query context
func getCacheKey(url string, qrmk string) string {
	// Use URL and encryption key to create unique cache key
	hash := md5.Sum([]byte(url + qrmk))
	return fmt.Sprintf("%x", hash)
}

// getCachedChunkPath returns the file path for a cached chunk
func getCachedChunkPath(cacheDir, cacheKey string) string {
	return filepath.Join(cacheDir, cacheKey+".chunk")
}

// isCacheValid checks if a cached chunk is still valid
func isCacheValid(filePath string, maxAge time.Duration) bool {
	info, err := os.Stat(filePath)
	if err != nil {
		return false
	}
	return time.Since(info.ModTime()) < maxAge
}

// ensureCacheDir creates the cache directory if it doesn't exist
func ensureCacheDir(cacheDir string) error {
	return os.MkdirAll(cacheDir, 0755)
}

// preloadAllChunks downloads and caches all chunks immediately
func preloadAllChunks(ctx context.Context, scd *snowflakeChunkDownloader, config ChunkCacheConfig) error {
	logger.WithContext(ctx).Infof("Preloading all %d chunks for caching", len(scd.ChunkMetas))

	// Ensure cache directory exists
	if err := ensureCacheDir(config.CacheDir); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	// Check which chunks need to be downloaded vs are already cached
	var chunksToDownload []int
	for i := range scd.ChunkMetas {
		idx := i
		cacheKey := getCacheKey(scd.ChunkMetas[idx].URL, scd.Qrmk)
		cachePath := getCachedChunkPath(config.CacheDir, cacheKey)

		if isCacheValid(cachePath, config.MaxAge) {
			logger.WithContext(ctx).Debugf("Chunk %d already cached: %s", idx+1, cachePath)
			// File is already cached and valid, no need to download
		} else {
			chunksToDownload = append(chunksToDownload, idx)
		}
	}

	if len(chunksToDownload) == 0 {
		logger.WithContext(ctx).Info("All chunks loaded from cache")
		return nil
	}

	logger.WithContext(ctx).Infof("Downloading %d chunks (not cached or stale)", len(chunksToDownload))

	// Set up concurrent download with error tracking using errgroup
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(MaxChunkDownloadWorkers)

	// Download all chunks that need downloading (raw bytes only, no decoding)
	for i := range chunksToDownload {
		idx := chunksToDownload[i]
		cacheKey := getCacheKey(scd.ChunkMetas[idx].URL, scd.Qrmk)
		g.Go(func() error {
			return downloadRawChunkToCache(ctx, scd, idx, config.CacheDir, cacheKey)
		})
	}

	// Wait for all downloads to complete
	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to download chunks: %w", err)
	}

	logger.WithContext(ctx).Info("All chunks successfully preloaded and cached")
	return nil
}

// downloadChunkHelperWithCache wraps the original downloadChunkHelper with local caching
// This is only used as a fallback if preloading fails
func downloadChunkHelperWithCache(config ChunkCacheConfig, originalFunc func(context.Context, *snowflakeChunkDownloader, int) error) func(context.Context, *snowflakeChunkDownloader, int) error {
	return func(ctx context.Context, scd *snowflakeChunkDownloader, idx int) error {
		// If caching is disabled, use original function
		if !config.Enabled {
			return originalFunc(ctx, scd, idx)
		}

		cacheKey := getCacheKey(scd.ChunkMetas[idx].URL, scd.Qrmk)
		cachePath := getCachedChunkPath(config.CacheDir, cacheKey)

		// Try to load from cache first
		if isCacheValid(cachePath, config.MaxAge) {
			logger.WithContext(ctx).Debugf("Loading chunk %d from cache: %s", idx+1, cachePath)
			if err := loadChunkFromCache(ctx, scd, idx, cachePath); err == nil {
				return nil
			} else {
				logger.WithContext(ctx).Warnf("Failed to load chunk %d from cache, falling back to normal download: %v", idx+1, err)
			}
		}

		// Fallback to original behavior (no caching)
		logger.WithContext(ctx).Debugf("Using original download behavior for chunk %d", idx+1)
		return originalFunc(ctx, scd, idx)
	}
}

// loadChunkFromCache loads a chunk from the local cache
func loadChunkFromCache(ctx context.Context, scd *snowflakeChunkDownloader, idx int, cachePath string) error {
	file, err := os.Open(cachePath)
	if err != nil {
		return fmt.Errorf("opening cache file: %w", err)
	}
	defer file.Close()

	bufStream := bufio.NewReader(file)
	return decodeChunk(ctx, scd, idx, bufStream)
}

// downloadRawChunkToCache downloads only the raw chunk bytes to cache without decoding
func downloadRawChunkToCache(ctx context.Context, scd *snowflakeChunkDownloader, idx int, cacheDir, cacheKey string) error {
	// Ensure cache directory exists
	if err := ensureCacheDir(cacheDir); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	// Prepare headers
	headers := make(map[string]string)
	if len(scd.ChunkHeader) > 0 {
		for k, v := range scd.ChunkHeader {
			headers[k] = v
		}
	} else {
		headers[headerSseCAlgorithm] = headerSseCAes
		headers[headerSseCKey] = scd.Qrmk
	}

	// Download chunk
	resp, err := scd.FuncGet(ctx, scd.sc, scd.ChunkMetas[idx].URL, headers, scd.sc.rest.RequestTimeout)
	if err != nil {
		return fmt.Errorf("getting chunk: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			logger.WithContext(ctx).Warnf("reading response body: %v", err)
		}
		logger.WithContext(ctx).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, scd.ChunkMetas[idx].URL, b)
		return &SnowflakeError{
			Number:      ErrFailedToGetChunk,
			SQLState:    SQLStateConnectionFailure,
			Message:     errMsgFailedToGetChunk,
			MessageArgs: []any{idx},
		}
	}

	// Create cache file
	cachePath := getCachedChunkPath(cacheDir, cacheKey)
	tempPath := cachePath + ".tmp"

	cacheFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("creating cache file: %w", err)
	}
	defer cacheFile.Close()

	// Copy raw bytes directly to cache file (no decoding)
	_, err = io.Copy(cacheFile, resp.Body)
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("writing cache file: %w", err)
	}

	// Atomically move temp file to final location
	if err := os.Rename(tempPath, cachePath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("moving cache file: %w", err)
	}

	logger.WithContext(ctx).Debugf("Raw cached chunk %d to: %s", idx+1, cachePath)
	return nil
}

// WithChunkCaching returns a context that enables chunk caching with the given configuration
func WithChunkCaching(ctx context.Context, config ChunkCacheConfig) context.Context {
	return context.WithValue(ctx, chunkCacheConfigKey, config)
}

// getChunkCacheConfig extracts chunk cache configuration from context
func getChunkCacheConfig(ctx context.Context) (ChunkCacheConfig, bool) {
	val := ctx.Value(chunkCacheConfigKey)
	if val == nil {
		return ChunkCacheConfig{}, false
	}
	config, ok := val.(ChunkCacheConfig)
	return config, ok
}

// chunkCacheConfigKey is the context key for chunk cache configuration
type chunkCacheConfigKeyType string

const chunkCacheConfigKey chunkCacheConfigKeyType = "chunk_cache_config"

// CleanChunkCache removes old cached chunks based on the provided configuration
func CleanChunkCache(config ChunkCacheConfig) error {
	if !config.Enabled {
		return nil
	}

	entries, err := os.ReadDir(config.CacheDir)
	if err != nil {
		return fmt.Errorf("reading cache directory: %w", err)
	}

	now := time.Now()
	var cleanedCount int

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".chunk") {
			continue
		}

		filePath := filepath.Join(config.CacheDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}

		if now.Sub(info.ModTime()) > config.MaxAge {
			if err := os.Remove(filePath); err != nil {
				logger.Warnf("Failed to remove old cache file %s: %v", filePath, err)
			} else {
				cleanedCount++
			}
		}
	}

	if cleanedCount > 0 {
		logger.Infof("Cleaned %d old cached chunks from %s", cleanedCount, config.CacheDir)
	}

	return nil
}

// ClearAllChunkCache removes all cached chunks regardless of age
func ClearAllChunkCache(config ChunkCacheConfig) error {
	if !config.Enabled {
		return nil
	}
	return os.RemoveAll(config.CacheDir)
}
