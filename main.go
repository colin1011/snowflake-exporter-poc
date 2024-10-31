package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/snowflakedb/gosnowflake"
)

type SnowflakeMetricsCollector struct {
	db *sql.DB

	// Prometheus metrics
	wareouseCredits *prometheus.Desc
	storageBytes    *prometheus.Desc
	queryCount      *prometheus.Desc
	concurrentQuery *prometheus.Desc
	
	mu sync.Mutex
}

func NewSnowflakeMetricsCollector(dsn string) (*SnowflakeMetricsCollector, error) {
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Snowflake: %v", err)
	}

	return &SnowflakeMetricsCollector{
		db: db,
		wareouseCredits: prometheus.NewDesc(
			"snowflake_warehouse_credits_used",
			"Number of credits used by warehouse",
			[]string{"warehouse_name"},
			nil,
		),
		storageBytes: prometheus.NewDesc(
			"snowflake_storage_bytes",
			"Total storage used in bytes",
			[]string{"database_name"},
			nil,
		),
		queryCount: prometheus.NewDesc(
			"snowflake_query_count",
			"Number of queries executed",
			[]string{"warehouse_name", "query_type"},
			nil,
		),
		concurrentQuery: prometheus.NewDesc(
			"snowflake_concurrent_queries",
			"Number of concurrent queries",
			[]string{"warehouse_name"},
			nil,
		),
	}, nil
}

func (c *SnowflakeMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.wareouseCredits
	ch <- c.storageBytes
	ch <- c.queryCount
	ch <- c.concurrentQuery
}

func (c *SnowflakeMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Warehouse Credits
	warehouseCreditsQuery := `
		SELECT warehouse_name, SUM(credits_used) as total_credits 
		FROM snowflake.account_usage.warehouse_metering_history 
		WHERE start_time > dateadd(day, -1, current_timestamp()) 
		GROUP BY warehouse_name
	`
	rows, err := c.db.Query(warehouseCreditsQuery)
	if err != nil {
		log.Printf("Error fetching warehouse credits: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var warehouseName string
		var creditsUsed float64
		if err := rows.Scan(&warehouseName, &creditsUsed); err != nil {
			log.Printf("Error scanning warehouse credits: %v", err)
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			c.wareouseCredits,
			prometheus.GaugeValue,
			creditsUsed,
			warehouseName,
		)
	}

	// Storage Bytes
	storageQuery := `
		SELECT database_name, storage_bytes 
		FROM snowflake.account_usage.database_storage_usage_history 
		WHERE usage_date = current_date()
	`
	rows, err = c.db.Query(storageQuery)
	if err != nil {
		log.Printf("Error fetching storage bytes: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var databaseName string
		var storageBytes float64
		if err := rows.Scan(&databaseName, &storageBytes); err != nil {
			log.Printf("Error scanning storage bytes: %v", err)
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			c.storageBytes,
			prometheus.GaugeValue,
			storageBytes,
			databaseName,
		)
	}
}

func main() {
	// Snowflake connection parameters
	dsn := fmt.Sprintf("%s:%s@%s/%s/%s/%s", 
		os.Getenv("SNOWFLAKE_USERNAME"), 
		os.Getenv("SNOWFLAKE_PASSWORD"), 
		os.Getenv("SNOWFLAKE_ACCOUNT"), 
		os.Getenv("SNOWFLAKE_DATABASE"), 
		os.Getenv("SNOWFLAKE_SCHEMA"), 
		os.Getenv("SNOWFLAKE_WAREHOUSE"),
	)

	// Create Snowflake metrics collector
	collector, err := NewSnowflakeMetricsCollector(dsn)
	if err != nil {
		log.Fatalf("Failed to create Snowflake metrics collector: %v", err)
	}

	// Register collector with Prometheus
	prometheus.MustRegister(collector)

	// Expose metrics endpoint
	http.Handle("/metrics", promhttp.Handler())
	
	// Start server
	port := os.Getenv("EXPORTER_PORT")
	if port == "" {
		port = "9090"
	}
	
	log.Printf("Starting Snowflake Prometheus Exporter on :%s", port)
	log.Fatal(http.ListenAndServe(":" + port, nil))
}