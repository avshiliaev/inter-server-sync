package dumper

import (
	"database/sql"
	"fmt"
	"github.com/moio/mgr-dump/schemareader"
	"strings"
)

type processItem struct {
	tableName string
	row       []rowDataStructure
	path      []string
}

func DumpTableFilter(db *sql.DB, tables []schemareader.Table, ids []int) DataDumper {

	tableMap := make(map[string]schemareader.Table)
	for _, table := range tables {
		tableMap[table.Name] = table
	}
	initalDataSet := make([]processItem, 0)
	for _, channelId := range ids {
		whereFilter := fmt.Sprintf("id = %d", channelId)
		sql := fmt.Sprintf(`SELECT * FROM rhnchannel where %s ;`, whereFilter)
		rows := executeQueryWithResults(db, sql)
		for _, row := range rows {
			initalDataSet = append(initalDataSet, processItem{tableMap["rhnchannel"].Name, row, []string{"rhnchannel"}})
		}

	}
	result := followTableLinks(db, tableMap, initalDataSet)
	return result
}

func followTableLinks(db *sql.DB, tableMap map[string]schemareader.Table, initialDataSet []processItem) DataDumper {

	result := DataDumper{make([]string, 0), make(map[string]TableFilter, 0)}

	itemsToProcess := initialDataSet

IterateItemsLoop:
	for i := 0; i < len(itemsToProcess); i++ {

		itemToProcess := itemsToProcess[i]
		table, ok := tableMap[itemToProcess.tableName]

		columnIndexes := make(map[string]int)
		for i, columnName := range table.Columns {
			columnIndexes[columnName] = i
		}
		resultTableValues, ok := result.TableKeys[table.Name]

		if ok {
			for _, tableKeyColumns := range resultTableValues.Keys {
				equalKey := true
				for columnName, rowIdColumnValue := range tableKeyColumns.key {
					if strings.Compare(rowIdColumnValue, formatField(itemToProcess.row[columnIndexes[columnName]])) != 0 {
						// ID already processed nothing to do
						equalKey = false
						break
					}
				}
				if equalKey {
					continue IterateItemsLoop
				}
			}
		} else {
			resultTableValues = TableFilter{TableName: table.Name, Keys: make([]TableKey, 0)}
		}

		key := make(map[string]string)
		if len(table.PKColumns) > 0 {
			for pkColumn, _ := range table.PKColumns {
				key[pkColumn] = formatField(itemToProcess.row[columnIndexes[pkColumn]])
			}
		} else {
			for _, pkColumn := range table.UniqueIndexes[table.MainUniqueIndexName].Columns {
				key[pkColumn] = formatField(itemToProcess.row[columnIndexes[pkColumn]])
			}
		}

		resultTableValues.Keys = append(resultTableValues.Keys, TableKey{key})
		result.TableKeys[table.Name] = resultTableValues

		itemsToProcess = append(itemsToProcess, followReferencesFrom(db, tableMap, table, columnIndexes, itemToProcess)...)
		////result.Queries = append(result.Queries, prepareRowInsert(db, table, row, tableMap, columnIndexes))
		//fmt.Println(prepareRowInsert(db, table, row, tableMap, columnIndexes))
		itemsToProcess = append(itemsToProcess, followReferencesTo(db, tableMap, table, columnIndexes, itemToProcess)...)

	}
	return result
}

func prepareRowInsert(db *sql.DB, table schemareader.Table, row []rowDataStructure, tableMap map[string]schemareader.Table, columnIndexes map[string]int) string {
	values := substitutePrimaryKey(table, row)
	values = substituteForeignKey(db, table, tableMap, values, columnIndexes)
	return generateInsertStatement(values, table)
}

func shouldFollowReferenceByLink(path []string, table schemareader.Table, reference schemareader.Reference, referencedTable schemareader.Table) bool {

	// if we already passed by the table we don't want to follow
	for _, p := range path {
		if strings.Compare(p, referencedTable.Name) == 0 {
			return false
		}
	}
	// HACK. We should not follow links to this table
	if strings.Compare(table.Name, "rhnpackagecapability") == 0 {
		return false
	}
	// HACK
	if strings.Compare(table.Name, "rhnpackage") == 0 && strings.Compare(referencedTable.Name, "rhnerratapackage") == 0 {
		return false
	}

	// maybe we can check by convention the linking table. example: rhnerrata -> rhnerratapackage

	// If we don't have a link from to this table we should try to use it.
	if len(referencedTable.ReferencedBy) == 0 {
		for _, ref := range referencedTable.References {
			//In the reference table we will go through all the references
			// ignoring the ones to the current table.
			// And see if we have already passed (part of path) in one of the references
			// If we already passed, we should not follow this path, because we have been already here from another reference
			if strings.Compare(table.Name, ref.TableName) != 0 {
				for _, p := range path {
					if strings.Compare(p, ref.TableName) == 0 {
						return false
					}
				}
			}
		}
		return true
	}
	return false
}

func followReferencesTo(db *sql.DB, tableMap map[string]schemareader.Table, table schemareader.Table, columnIndexes map[string]int, row processItem) []processItem {
	result := make([]processItem, 0)

	for _, reference := range table.ReferencedBy {
		referencedTable, ok := tableMap[reference.TableName]
		if !ok {
			continue
		}
		if !shouldFollowReferenceByLink(row.path, table, reference, referencedTable) {
			continue
		}

		localColumns := make([]string, 0)
		foreignColumns := make([]string, 0)

		whereParameters := make([]string, 0)
		scanParameters := make([]interface{}, 0)
		for localColumn, foreignColumn := range reference.ColumnMapping {
			localColumns = append(localColumns, localColumn)
			foreignColumns = append(foreignColumns, foreignColumn)

			whereParameters = append(whereParameters, fmt.Sprintf("%s = $%d", localColumn, len(whereParameters)+1))
			scanParameters = append(scanParameters, row.row[columnIndexes[foreignColumn]].value)
		}

		formattedColumns := strings.Join(referencedTable.Columns, ", ")
		formatedWhereParameters := strings.Join(whereParameters, " and ")
		sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formatedWhereParameters)
		followRows := executeQueryWithResults(db, sql, scanParameters...)

		if len(followRows) > 0 {
			for _, followRow := range followRows {
				result = append(result, processItem{referencedTable.Name, followRow, append(row.path, referencedTable.Name)})
			}
		}
	}

	return result
}

func followReferencesFrom(db *sql.DB, tableMap map[string]schemareader.Table, table schemareader.Table, columnIndexes map[string]int, row processItem) []processItem {
	result := make([]processItem, 0)
	for _, reference := range table.References {
		foreignTable, tableExist := tableMap[reference.TableName]
		if !tableExist {
			continue
		}
		passed := false
		for _, p := range row.path {
			if strings.Compare(p, foreignTable.Name) == 0 {
				passed = true
				break
			}
		}
		if passed {
			continue
		}

		localColumns := make([]string, 0)
		foreignColumns := make([]string, 0)

		whereParameters := make([]string, 0)
		scanParameters := make([]interface{}, 0)
		for localColumn, foreignColumn := range reference.ColumnMapping {
			localColumns = append(localColumns, localColumn)
			foreignColumns = append(foreignColumns, foreignColumn)

			whereParameters = append(whereParameters, fmt.Sprintf("%s = $%d", foreignColumn, len(whereParameters)+1))
			scanParameters = append(scanParameters, row.row[columnIndexes[localColumn]].value)
		}

		formattedColumns := strings.Join(foreignTable.Columns, ", ")
		formatedWhereParameters := strings.Join(whereParameters, " and ")
		sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s;`, formattedColumns, reference.TableName, formatedWhereParameters)
		followRows := executeQueryWithResults(db, sql, scanParameters...)

		if len(followRows) > 0 {
			for _, followRow := range followRows {
				result = append(result, processItem{foreignTable.Name, followRow, append(row.path, foreignTable.Name)})
			}
		}
	}
	return result
}
