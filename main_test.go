package main

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	// Mock SQL driver
	"github.com/DATA-DOG/go-sqlmock"
)

// MockDB is a mock implementation of database/sql.DB
type MockDB struct {
	mock.Mock
}

func (m *MockDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	args = append([]interface{}{query}, args...)
	returnArgs := m.Called(args...)
	return returnArgs.Get(0).(*sql.Rows), returnArgs.Error(1)
}

func TestSnowflakeMetricsCollector_Collect(t *testing.T) {
	// Create a sqlmock database
	db, mock, err := sqlmock.Open()
	assert.NoError(t, err)
	defer db.Close()

	// Prepare mock rows for warehouse credits
	warehouseCreditRows := sqlmock.NewRows([]string{"warehouse_name", "total_credits"}).
		AddRow("COMPUTE_WH", 10.5).
		AddRow("REPORTING_WH", 5.2)

	// Prepare mock rows for storage bytes
	storageRows := sqlmock.NewRows([]string{"database_name", "storage_bytes"}).
		AddRow("PROD_DB", 1024000).
		AddRow("DEV_DB", 512000)

	// Expect queries
	mock.ExpectQuery("SELECT warehouse_name, SUM\\(credits_used\\) as total_credits").
		WillReturnRows(warehouseCreditRows)

	mock.ExpectQuery("SELECT database_name, storage_bytes").
		WillReturnRows(storageRows)

	// Create collector with mock DB
	collector := &SnowflakeMetricsCollector{
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
	}

	// Collect metrics
	ch := make(chan prometheus.Metric, 10)
	collector.Collect(ch)
	close(ch)

	// Validate warehouse credits metrics
	expectedWarehouseCredits := `
		# HELP snowflake_warehouse_credits_used Number of credits used by warehouse
		# TYPE snowflake_warehouse_credits_used gauge
		snowflake_warehouse_credits_used{warehouse_name="COMPUTE_WH"} 10.5
		snowflake_warehouse_credits_used{warehouse_name="REPORTING_WH"} 5.2
	`
	err = testutil.CollectAndCompare(collector,
		strings.NewReader(expectedWarehouseCredits),
		"snowflake_warehouse_credits_used")
	assert.NoError(t, err)

	// Validate storage bytes metrics
	expectedStorageBytes := `
		# HELP snowflake_storage_bytes Total storage used in bytes
		# TYPE snowflake_storage_bytes gauge
		snowflake_storage_bytes{database_name="PROD_DB"} 1024000
		snowflake_storage_bytes{database_name="DEV_DB"} 512000
	`
	err = testutil.CollectAndCompare(collector,
		strings.NewReader(expectedStorageBytes),
		"snowflake_storage_bytes")
	assert.NoError(t, err)
}

func TestSnowflakeMetricsCollector_QueryError(t *testing.T) {
	// Create a sqlmock database
	db, mock, err := sqlmock.Open()
	assert.NoError(t, err)
	defer db.Close()

	// Expect query to fail
	mock.ExpectQuery("SELECT warehouse_name, SUM\\(credits_used\\) as total_credits").
		WillReturnError(fmt.Errorf("database connection error"))

	// Create collector with mock DB
	collector := &SnowflakeMetricsCollector{
		db: db,
		wareouseCredits: prometheus.NewDesc(
			"snowflake_warehouse_credits_used",
			"Number of credits used by warehouse",
			[]string{"warehouse_name"},
			nil,
		),
	}

	// Collect metrics (should handle error gracefully)
	ch := make(chan prometheus.Metric, 10)
	collector.Collect(ch)
	close(ch)

	// Verify no metrics were sent
	assert.Equal(t, 0, len(ch))
}

func TestSnowflakeMetricsCollector_Describe(t *testing.T) {
	// Create a mock database (not used in Describe)
	db, _, err := sqlmock.Open()
	assert.NoError(t, err)
	defer db.Close()

	// Create collector
	collector := &SnowflakeMetricsCollector{
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
	}

	// Channel to receive descriptions
	ch := make(chan *prometheus.Desc, 10)
	collector.Describe(ch)
	close(ch)

	// Collect descriptions
	var descriptions []string
	for desc := range ch {
		descriptions = append(descriptions, desc.String())
	}

	// Verify correct number of descriptions
	assert.Equal(t, 2, len(descriptions))
	assert.Contains(t, descriptions[0], "snowflake_warehouse_credits_used")
	assert.Contains(t, descriptions[1], "snowflake_storage_bytes")
}
