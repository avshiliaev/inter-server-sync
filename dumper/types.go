package dumper

import (
	"github.com/rs/zerolog/log"
	"github.com/uyuni-project/inter-server-sync/sqlUtil"
	"sync"
	"time"
)

type RowKey struct {
	Column string
	Value  string
}

type TableKey struct {
	Key []RowKey
}

type TableDump struct {
	TableName string
	KeyMap    map[string]bool
	Keys      []TableKey
}

type DataDumper struct {
	TableData            map[string]TableDump
	Paths                map[string]bool
	stopLogging          chan bool
	totalExportedRecords int
	mu                   sync.Mutex
}

func NewDataDumper() *DataDumper {
	return &DataDumper{
		TableData:   make(map[string]TableDump, 0),
		Paths:       make(map[string]bool),
		stopLogging: make(chan bool),
	}
}

func (d *DataDumper) StartLogTableSizes(totalSize int) {
	if log.Logger.Debug().Enabled() {
		ticker := time.NewTicker(30 * time.Second)
		go func() {
			count := 0
			for {
				select {
				case <-ticker.C:
					keysSize := 0
					maxSize := 0
					table := ""
					for key, value := range d.TableData {
						keysSize = keysSize + len(value.KeyMap)
						if len(value.KeyMap) > maxSize {
							maxSize = len(value.KeyMap)
							table = key
						}
					}
					log.
						Debug().
						Msgf(
							"#count: %d #rowsToProcess: #%d ;  #rowsToExport: #%d  --> Bigger export table: %s: #%d",
							count, totalSize, keysSize, table, maxSize,
						)
					if totalSize == 0 {
						break
					}
					count++
				case <-d.stopLogging:
					ticker.Stop()
					return
				}
			}
		}()
	}
}

func (d *DataDumper) StartLogWrittenRows() {
	if log.Logger.Debug().Enabled() {
		ticker := time.NewTicker(30 * time.Second)
		go func() {
			count := 0
			totalRecords := 0

			for _, value := range d.TableData {
				totalRecords = totalRecords + len(value.Keys)
			}

			for {
				select {
				case <-ticker.C:
					{
						log.Debug().Msgf(
							"#count: %d #cacheSize %d -- #writtenRows: #%d of %d",
							count, len(cache), d.totalExportedRecords, totalRecords,
						)
						count++
					}
				case <-d.stopLogging:
					return
				}
			}
		}()
	}
}

func (d *DataDumper) UpdateTotalExported(numProcessed int) {
	if log.Logger.Debug().Enabled() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.totalExportedRecords += numProcessed
	}
}

func (d *DataDumper) StopLog() {
	if log.Logger.Debug().Enabled() {
		d.stopLogging <- true
	}
}

type processItem struct {
	tableName string
	row       []sqlUtil.RowDataStructure
	path      []string
}

type PrintSqlOptions struct {
	TablesToClean            []string
	CleanWhereClause         string
	OnlyIfParentExistsTables []string
	MemoryProfileFolder      string
}
