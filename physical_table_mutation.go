package rovadb

import (
	"reflect"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/parser"
	"github.com/Khorlane/RovaDB/internal/storage"
)

// physical_table_mutation.go keeps page-level table mutation choreography out
// of the public API entrypoint file while preserving the same root-layer
// package ownership and behavior.

func (db *DB) applyStagedTableRewrite(stagedTables map[string]*executor.Table, tableName string) error {
	return db.applyStagedTableRewrites(stagedTables, []string{tableName})
}

func (db *DB) applyStagedTableRewrites(stagedTables map[string]*executor.Table, tableNames []string) error {
	if db == nil || db.pager == nil {
		return nil
	}
	if len(tableNames) == 0 {
		return nil
	}

	originalFreeListHead := db.freeListHead
	pages := make([]stagedPage, 0)
	nextFreshID := db.pager.NextPageID()
	replacedPageIDs := make([]storage.PageID, 0)

	for _, tableName := range tableNames {
		table := stagedTables[tableName]
		if table == nil {
			continue
		}

		// DELETE and wider persisted row-content rewrites still use a full physical
		// table rewrite through the committed TableHeader/SpaceMap/Data model when a
		// narrower path is not in play.
		table.SetStorageMeta(table.RootPageID(), uint32(len(table.Rows)))
		spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
		if err != nil {
			db.freeListHead = originalFreeListHead
			return err
		}
		replacedPageIDs = append(replacedPageIDs, spaceMapPageIDs...)
		replacedPageIDs = append(replacedPageIDs, dataPageIDs...)

		tablePages, locators, err := db.stageTableRewriteViaPhysicalStorage(table, table.Rows, false, nextFreshID, false)
		if err != nil {
			db.freeListHead = originalFreeListHead
			return err
		}
		pages = append(pages, tablePages...)
		nextFreshID = nextFreshPageIDAfter(pages, db.pager.NextPageID())

		indexPages, err := db.buildRebuiltIndexPages(table, table.Rows, locators, nextFreshID)
		if err != nil {
			db.freeListHead = originalFreeListHead
			return err
		}
		pages = append(pages, indexPages...)
		nextFreshID = nextFreshPageIDAfter(pages, db.pager.NextPageID())
	}

	if len(replacedPageIDs) != 0 {
		freedPages, err := db.buildFreedPages(replacedPageIDs...)
		if err != nil {
			db.freeListHead = originalFreeListHead
			return err
		}
		pages = append(pages, freedPages...)
	}

	catalogData, err := db.buildCatalogPageData(stagedTables, pages)
	if err != nil {
		db.freeListHead = originalFreeListHead
		return wrapStorageError(err)
	}

	if err := db.stageDirtyState(catalogData, pages); err != nil {
		db.freeListHead = originalFreeListHead
		return err
	}
	return nil
}

func (db *DB) applyStagedUpdate(stagedTables map[string]*executor.Table, tableName string) error {
	if db == nil || db.pager == nil {
		return nil
	}

	table := stagedTables[tableName]
	if table == nil {
		return nil
	}
	committedTable := db.tables[tableName]
	if committedTable == nil {
		return newExecError("table not found: " + tableName)
	}

	table.SetStorageMeta(table.RootPageID(), uint32(len(table.Rows)))
	originalFreeListHead := db.freeListHead
	pages, locators, err := db.stageTableUpdateViaPhysicalStorage(committedTable, table)
	if err != nil {
		db.freeListHead = originalFreeListHead
		return err
	}

	indexPages, err := db.buildRebuiltIndexPages(table, table.Rows, locators, nextFreshPageIDAfter(pages, db.pager.NextPageID()))
	if err != nil {
		db.freeListHead = originalFreeListHead
		return err
	}
	pages = append(pages, indexPages...)

	catalogData, err := db.buildCatalogPageData(stagedTables, pages)
	if err != nil {
		db.freeListHead = originalFreeListHead
		return wrapStorageError(err)
	}
	return db.stageDirtyState(catalogData, pages)
}

func (db *DB) applyStagedInsert(stagedTables map[string]*executor.Table, tableName string) error {
	if db == nil || db.pager == nil {
		return nil
	}

	table := stagedTables[tableName]
	if table == nil {
		return nil
	}

	table.SetStorageMeta(table.RootPageID(), uint32(len(table.Rows)))
	if len(table.Rows) == 0 {
		return newStorageError("row locator mismatch")
	}
	originalFreeListHead := db.freeListHead
	pages, locator, err := db.stageTableInsertViaPhysicalStorage(table, table.Rows[len(table.Rows)-1])
	if err != nil {
		db.freeListHead = originalFreeListHead
		return err
	}

	indexPages, err := db.buildInsertedIndexPages(table, table.Rows[len(table.Rows)-1], locator, nextFreshPageIDAfter(pages, db.pager.NextPageID()))
	if err != nil {
		db.freeListHead = originalFreeListHead
		return err
	}
	pages = append(pages, indexPages...)

	catalogData, err := db.buildCatalogPageData(stagedTables, pages)
	if err != nil {
		db.freeListHead = originalFreeListHead
		return wrapStorageError(err)
	}
	return db.stageDirtyState(catalogData, pages)
}

func stagePageImage(staged map[storage.PageID]stagedPage, order *[]storage.PageID, id storage.PageID, data []byte, isNew bool) {
	if _, exists := staged[id]; !exists {
		*order = append(*order, id)
	}
	staged[id] = stagedPage{
		id:    id,
		data:  data,
		isNew: isNew,
	}
}

func (db *DB) mutablePageImage(staged map[storage.PageID]stagedPage, order *[]storage.PageID, pageID storage.PageID) ([]byte, error) {
	if existing, ok := staged[pageID]; ok {
		return existing.data, nil
	}
	pageData, err := db.pager.ReadPage(pageID)
	if err != nil {
		return nil, wrapStorageError(err)
	}
	cloned := append([]byte(nil), pageData...)
	stagePageImage(staged, order, pageID, cloned, false)
	return cloned, nil
}

func (db *DB) allocatePageImage(nextFreshID *storage.PageID, staged map[storage.PageID]stagedPage, order *[]storage.PageID, init func(pageID storage.PageID) []byte) (storage.PageID, []byte, error) {
	pageID, isNew, err := db.allocatePageIDFrom(nextFreshID)
	if err != nil {
		return 0, nil, err
	}
	pageData := init(pageID)
	stagePageImage(staged, order, pageID, pageData, isNew)
	return pageID, pageData, nil
}

func (db *DB) stageTableInsertViaPhysicalStorage(table *executor.Table, row []parser.Value) ([]stagedPage, storage.RowLocator, error) {
	nextFreshID := db.pager.NextPageID()
	staged := make(map[storage.PageID]stagedPage)
	order := make([]storage.PageID, 0)
	locator, err := db.insertRowIntoPhysicalStorageState(table, row, &nextFreshID, staged, &order)
	if err != nil {
		return nil, storage.RowLocator{}, err
	}
	return finalizedStagedPages(staged, order), locator, nil
}

func (db *DB) stageTableUpdateViaPhysicalStorage(committedTable, updatedTable *executor.Table) ([]stagedPage, []storage.RowLocator, error) {
	if db == nil || committedTable == nil || updatedTable == nil {
		return nil, nil, ErrInvalidArgument
	}
	oldLocators, oldRows, err := loadCommittedTableRowsAndLocators(db.pool, committedTable)
	if err != nil {
		return nil, nil, err
	}
	if len(oldRows) != len(updatedTable.Rows) || len(oldLocators) != len(updatedTable.Rows) {
		return nil, nil, newStorageError("row locator mismatch")
	}

	nextFreshID := db.pager.NextPageID()
	staged := make(map[storage.PageID]stagedPage)
	order := make([]storage.PageID, 0)
	finalLocators := append([]storage.RowLocator(nil), oldLocators...)
	for rowIndex, oldRow := range oldRows {
		if reflect.DeepEqual(oldRow, updatedTable.Rows[rowIndex]) {
			continue
		}
		newLocator, err := db.stageUpdatedRowViaPhysicalStorage(updatedTable, oldLocators[rowIndex], updatedTable.Rows[rowIndex], &nextFreshID, staged, &order)
		if err != nil {
			return nil, nil, err
		}
		finalLocators[rowIndex] = newLocator
	}
	return finalizedStagedPages(staged, order), finalLocators, nil
}

func (db *DB) stageTableRewriteViaPhysicalStorage(table *executor.Table, rows [][]parser.Value, allowFreshHeaderBootstrap bool, startNextFreshID storage.PageID, freeReplacedPages bool) ([]stagedPage, []storage.RowLocator, error) {
	if db == nil || table == nil {
		return nil, nil, ErrInvalidArgument
	}
	nextFreshID := startNextFreshID
	staged := make(map[storage.PageID]stagedPage)
	order := make([]storage.PageID, 0)

	spaceMapPageIDs, dataPageIDs, err := committedTablePhysicalStorageInventory(db.pool, table)
	if err != nil {
		return nil, nil, err
	}
	headerPage, err := db.mutablePageImage(staged, &order, table.TableHeaderPageID())
	if err != nil {
		return nil, nil, err
	}
	if err := storage.ValidateTableHeaderPage(headerPage); err != nil {
		if !allowFreshHeaderBootstrap || table.TableHeaderPageID() == 0 || table.TableID == 0 || table.FirstSpaceMapPageID() != 0 || table.OwnedDataPageCount() != 0 || table.OwnedSpaceMapPageCount() != 0 {
			return nil, nil, wrapStorageError(err)
		}
		headerPage = storage.InitTableHeaderPage(uint32(table.TableHeaderPageID()), table.TableID)
		stagePageImage(staged, &order, table.TableHeaderPageID(), headerPage, true)
		if table.TableHeaderPageID() >= nextFreshID {
			nextFreshID = table.TableHeaderPageID() + 1
		}
	}
	if err := storage.SetTableHeaderOwnedDataPageCount(headerPage, 0); err != nil {
		return nil, nil, wrapStorageError(err)
	}
	if err := storage.SetTableHeaderOwnedSpaceMapPageCount(headerPage, 0); err != nil {
		return nil, nil, wrapStorageError(err)
	}
	if err := storage.SetTableHeaderFirstSpaceMapPageID(headerPage, 0); err != nil {
		return nil, nil, wrapStorageError(err)
	}
	table.SetPhysicalTableRootMeta(table.TableHeaderPageID(), table.TableStorageFormatVersion(), 0, 0, 0)

	locators := make([]storage.RowLocator, 0, len(rows))
	for _, row := range rows {
		locator, err := db.insertRowIntoPhysicalStorageState(table, row, &nextFreshID, staged, &order)
		if err != nil {
			return nil, nil, err
		}
		locators = append(locators, locator)
	}
	if freeReplacedPages && (len(spaceMapPageIDs) != 0 || len(dataPageIDs) != 0) {
		freedPages, err := db.buildFreedPages(append(spaceMapPageIDs, dataPageIDs...)...)
		if err != nil {
			return nil, nil, err
		}
		for _, freed := range freedPages {
			stagePageImage(staged, &order, freed.id, freed.data, freed.isNew)
		}
	}
	return finalizedStagedPages(staged, order), locators, nil
}

func finalizedStagedPages(staged map[storage.PageID]stagedPage, order []storage.PageID) []stagedPage {
	pages := make([]stagedPage, 0, len(order))
	for _, pageID := range order {
		pages = append(pages, staged[pageID])
	}
	return pages
}

func (db *DB) insertRowIntoPhysicalStorageState(table *executor.Table, row []parser.Value, nextFreshID *storage.PageID, staged map[storage.PageID]stagedPage, order *[]storage.PageID) (storage.RowLocator, error) {
	if db == nil || table == nil || nextFreshID == nil {
		return storage.RowLocator{}, ErrInvalidArgument
	}
	if table.TableHeaderPageID() == 0 {
		return storage.RowLocator{}, newStorageError("corrupted header page")
	}

	headerPage, err := db.mutablePageImage(staged, order, table.TableHeaderPageID())
	if err != nil {
		return storage.RowLocator{}, err
	}
	if err := storage.ValidateTableHeaderPage(headerPage); err != nil {
		return storage.RowLocator{}, wrapStorageError(err)
	}

	rowBytes, err := storage.EncodeSlottedRow(storageValuesFromParser(row), storageColumnTypes(table.Columns))
	if err != nil {
		return storage.RowLocator{}, wrapStorageError(err)
	}

	firstSpaceMapID := table.FirstSpaceMapPageID()
	var targetDataPage storage.PageID
	var targetSpaceMapPage storage.PageID
	targetEntryID := -1
	lastSpaceMapPageID := storage.PageID(0)

	for currentSpaceMapID := firstSpaceMapID; currentSpaceMapID != 0; {
		lastSpaceMapPageID = currentSpaceMapID
		spaceMapPage, err := db.mutablePageImage(staged, order, currentSpaceMapID)
		if err != nil {
			return storage.RowLocator{}, err
		}
		if err := storage.ValidateSpaceMapPage(spaceMapPage); err != nil {
			return storage.RowLocator{}, wrapStorageError(err)
		}
		owningTableID, err := storage.SpaceMapOwningTableID(spaceMapPage)
		if err != nil {
			return storage.RowLocator{}, wrapStorageError(err)
		}
		if owningTableID != table.TableID {
			return storage.RowLocator{}, newStorageError("corrupted space map page")
		}
		entryCount, err := storage.SpaceMapEntryCount(spaceMapPage)
		if err != nil {
			return storage.RowLocator{}, wrapStorageError(err)
		}
		for entryID := 0; entryID < entryCount; entryID++ {
			entry, err := storage.SpaceMapPageEntry(spaceMapPage, entryID)
			if err != nil {
				return storage.RowLocator{}, wrapStorageError(err)
			}
			if entry.FreeSpaceBucket == storage.SpaceMapBucketFull {
				continue
			}
			dataPage, err := db.mutablePageImage(staged, order, entry.DataPageID)
			if err != nil {
				return storage.RowLocator{}, err
			}
			if err := storage.ValidateOwnedDataPage(dataPage, table.TableID); err != nil {
				return storage.RowLocator{}, wrapStorageError(err)
			}
			fit, err := storage.CanFitRow(dataPage, len(rowBytes))
			if err != nil {
				return storage.RowLocator{}, wrapStorageError(err)
			}
			if !fit {
				bucket, err := storage.TablePageFreeSpaceBucket(dataPage)
				if err != nil {
					return storage.RowLocator{}, wrapStorageError(err)
				}
				if bucket != entry.FreeSpaceBucket {
					if err := storage.UpdateSpaceMapEntry(spaceMapPage, entryID, storage.SpaceMapEntry{
						DataPageID:      entry.DataPageID,
						FreeSpaceBucket: bucket,
					}); err != nil {
						return storage.RowLocator{}, wrapStorageError(err)
					}
				}
				continue
			}
			targetDataPage = entry.DataPageID
			targetSpaceMapPage = currentSpaceMapID
			targetEntryID = entryID
			break
		}
		if targetDataPage != 0 {
			break
		}
		nextSpaceMapID, err := storage.SpaceMapNextPageID(spaceMapPage)
		if err != nil {
			return storage.RowLocator{}, wrapStorageError(err)
		}
		currentSpaceMapID = storage.PageID(nextSpaceMapID)
	}

	var dataPage []byte
	if targetDataPage == 0 {
		var spaceMapPage []byte
		if firstSpaceMapID == 0 {
			targetSpaceMapPage, spaceMapPage, err = db.allocatePageImage(nextFreshID, staged, order, func(pageID storage.PageID) []byte {
				return storage.InitSpaceMapPage(uint32(pageID), table.TableID)
			})
			if err != nil {
				return storage.RowLocator{}, err
			}
			if err := storage.SetTableHeaderFirstSpaceMapPageID(headerPage, uint32(targetSpaceMapPage)); err != nil {
				return storage.RowLocator{}, wrapStorageError(err)
			}
			if err := storage.SetTableHeaderOwnedSpaceMapPageCount(headerPage, table.OwnedSpaceMapPageCount()+1); err != nil {
				return storage.RowLocator{}, wrapStorageError(err)
			}
			table.SetPhysicalTableRootMeta(table.TableHeaderPageID(), table.TableStorageFormatVersion(), targetSpaceMapPage, table.OwnedDataPageCount(), table.OwnedSpaceMapPageCount()+1)
		} else {
			spaceMapPage, err = db.mutablePageImage(staged, order, lastSpaceMapPageID)
			if err != nil {
				return storage.RowLocator{}, err
			}
			entryCount, err := storage.SpaceMapEntryCount(spaceMapPage)
			if err != nil {
				return storage.RowLocator{}, wrapStorageError(err)
			}
			if entryCount >= storage.SpaceMapPageEntryCapacity() {
				targetSpaceMapPage, _, err = db.allocatePageImage(nextFreshID, staged, order, func(pageID storage.PageID) []byte {
					return storage.InitSpaceMapPage(uint32(pageID), table.TableID)
				})
				if err != nil {
					return storage.RowLocator{}, err
				}
				if err := storage.SetSpaceMapNextPageID(spaceMapPage, uint32(targetSpaceMapPage)); err != nil {
					return storage.RowLocator{}, wrapStorageError(err)
				}
				spaceMapPage, err = db.mutablePageImage(staged, order, targetSpaceMapPage)
				if err != nil {
					return storage.RowLocator{}, err
				}
				if err := storage.SetTableHeaderOwnedSpaceMapPageCount(headerPage, table.OwnedSpaceMapPageCount()+1); err != nil {
					return storage.RowLocator{}, wrapStorageError(err)
				}
				table.SetPhysicalTableRootMeta(table.TableHeaderPageID(), table.TableStorageFormatVersion(), table.FirstSpaceMapPageID(), table.OwnedDataPageCount(), table.OwnedSpaceMapPageCount()+1)
			} else {
				targetSpaceMapPage = lastSpaceMapPageID
			}
		}

		targetDataPage, dataPage, err = db.allocatePageImage(nextFreshID, staged, order, func(pageID storage.PageID) []byte {
			return storage.InitOwnedDataPage(uint32(pageID), table.TableID)
		})
		if err != nil {
			return storage.RowLocator{}, err
		}
		slotID, err := storage.InsertRowIntoTablePage(dataPage, rowBytes)
		if err != nil {
			return storage.RowLocator{}, wrapStorageError(err)
		}
		bucket, err := storage.TablePageFreeSpaceBucket(dataPage)
		if err != nil {
			return storage.RowLocator{}, wrapStorageError(err)
		}
		spaceMapPage, err = db.mutablePageImage(staged, order, targetSpaceMapPage)
		if err != nil {
			return storage.RowLocator{}, err
		}
		entryID, err := storage.AppendSpaceMapEntry(spaceMapPage, storage.SpaceMapEntry{
			DataPageID:      targetDataPage,
			FreeSpaceBucket: bucket,
		})
		if err != nil {
			return storage.RowLocator{}, wrapStorageError(err)
		}
		targetEntryID = entryID
		if err := storage.SetTableHeaderOwnedDataPageCount(headerPage, table.OwnedDataPageCount()+1); err != nil {
			return storage.RowLocator{}, wrapStorageError(err)
		}
		table.SetPhysicalTableRootMeta(table.TableHeaderPageID(), table.TableStorageFormatVersion(), table.FirstSpaceMapPageID(), table.OwnedDataPageCount()+1, table.OwnedSpaceMapPageCount())
		return storage.RowLocator{PageID: uint32(targetDataPage), SlotID: uint16(slotID)}, nil
	}

	dataPage, err = db.mutablePageImage(staged, order, targetDataPage)
	if err != nil {
		return storage.RowLocator{}, err
	}
	slotID, err := storage.InsertRowIntoTablePage(dataPage, rowBytes)
	if err != nil {
		return storage.RowLocator{}, wrapStorageError(err)
	}
	bucket, err := storage.TablePageFreeSpaceBucket(dataPage)
	if err != nil {
		return storage.RowLocator{}, wrapStorageError(err)
	}
	spaceMapPage, err := db.mutablePageImage(staged, order, targetSpaceMapPage)
	if err != nil {
		return storage.RowLocator{}, err
	}
	if err := storage.UpdateSpaceMapEntry(spaceMapPage, targetEntryID, storage.SpaceMapEntry{
		DataPageID:      targetDataPage,
		FreeSpaceBucket: bucket,
	}); err != nil {
		return storage.RowLocator{}, wrapStorageError(err)
	}
	return storage.RowLocator{PageID: uint32(targetDataPage), SlotID: uint16(slotID)}, nil
}

func (db *DB) stageUpdatedRowViaPhysicalStorage(table *executor.Table, locator storage.RowLocator, row []parser.Value, nextFreshID *storage.PageID, staged map[storage.PageID]stagedPage, order *[]storage.PageID) (storage.RowLocator, error) {
	if db == nil || table == nil || nextFreshID == nil {
		return storage.RowLocator{}, ErrInvalidArgument
	}
	pageID := storage.PageID(locator.PageID)
	if pageID == 0 {
		return storage.RowLocator{}, newStorageError("corrupted table page")
	}

	dataPage, err := db.mutablePageImage(staged, order, pageID)
	if err != nil {
		return storage.RowLocator{}, err
	}
	if err := storage.ValidateOwnedDataPage(dataPage, table.TableID); err != nil {
		return storage.RowLocator{}, wrapStorageError(err)
	}

	rowBytes, err := storage.EncodeSlottedRow(storageValuesFromParser(row), storageColumnTypes(table.Columns))
	if err != nil {
		return storage.RowLocator{}, wrapStorageError(err)
	}

	fit, err := storage.CanUpdateRowInPlace(dataPage, int(locator.SlotID), len(rowBytes))
	if err != nil {
		return storage.RowLocator{}, wrapStorageError(err)
	}
	if fit {
		if err := storage.UpdateRowBySlot(dataPage, int(locator.SlotID), rowBytes); err != nil {
			return storage.RowLocator{}, wrapStorageError(err)
		}
		if err := db.refreshSpaceMapBucketForDataPage(table, pageID, staged, order); err != nil {
			return storage.RowLocator{}, err
		}
		return locator, nil
	}

	if err := storage.DeleteRowBySlot(dataPage, int(locator.SlotID)); err != nil {
		return storage.RowLocator{}, wrapStorageError(err)
	}
	liveRows, err := storage.TablePageLiveRowCount(dataPage)
	if err != nil {
		return storage.RowLocator{}, wrapStorageError(err)
	}
	var reclaimedPageIDs []storage.PageID
	if liveRows == 0 {
		reclaimedPageIDs, err = db.removeOwnedDataPageFromPhysicalStorageState(table, pageID, staged, order)
		if err != nil {
			return storage.RowLocator{}, err
		}
	} else {
		if err := db.refreshSpaceMapBucketForDataPage(table, pageID, staged, order); err != nil {
			return storage.RowLocator{}, err
		}
	}
	newLocator, err := db.insertRowIntoPhysicalStorageState(table, row, nextFreshID, staged, order)
	if err != nil {
		return storage.RowLocator{}, err
	}
	if len(reclaimedPageIDs) != 0 {
		freedPages, err := db.buildFreedPages(reclaimedPageIDs...)
		if err != nil {
			return storage.RowLocator{}, err
		}
		for _, freed := range freedPages {
			stagePageImage(staged, order, freed.id, freed.data, freed.isNew)
		}
	}
	return newLocator, nil
}

func (db *DB) refreshSpaceMapBucketForDataPage(table *executor.Table, dataPageID storage.PageID, staged map[storage.PageID]stagedPage, order *[]storage.PageID) error {
	if db == nil || table == nil {
		return ErrInvalidArgument
	}
	if table.FirstSpaceMapPageID() == 0 || dataPageID == 0 {
		return newStorageError("corrupted space map page")
	}

	dataPage, err := db.mutablePageImage(staged, order, dataPageID)
	if err != nil {
		return err
	}
	if err := storage.ValidateOwnedDataPage(dataPage, table.TableID); err != nil {
		return wrapStorageError(err)
	}
	bucket, err := storage.TablePageFreeSpaceBucket(dataPage)
	if err != nil {
		return wrapStorageError(err)
	}

	for spaceMapPageID := table.FirstSpaceMapPageID(); spaceMapPageID != 0; {
		spaceMapPage, err := db.mutablePageImage(staged, order, spaceMapPageID)
		if err != nil {
			return err
		}
		if err := storage.ValidateSpaceMapPage(spaceMapPage); err != nil {
			return wrapStorageError(err)
		}
		owningTableID, err := storage.SpaceMapOwningTableID(spaceMapPage)
		if err != nil {
			return wrapStorageError(err)
		}
		if owningTableID != table.TableID {
			return newStorageError("corrupted space map page")
		}
		entryCount, err := storage.SpaceMapEntryCount(spaceMapPage)
		if err != nil {
			return wrapStorageError(err)
		}
		for entryID := 0; entryID < entryCount; entryID++ {
			entry, err := storage.SpaceMapPageEntry(spaceMapPage, entryID)
			if err != nil {
				return wrapStorageError(err)
			}
			if entry.DataPageID != dataPageID {
				continue
			}
			if entry.FreeSpaceBucket == bucket {
				return nil
			}
			if err := storage.UpdateSpaceMapEntry(spaceMapPage, entryID, storage.SpaceMapEntry{
				DataPageID:      dataPageID,
				FreeSpaceBucket: bucket,
			}); err != nil {
				return wrapStorageError(err)
			}
			return nil
		}
		nextPageID, err := storage.SpaceMapNextPageID(spaceMapPage)
		if err != nil {
			return wrapStorageError(err)
		}
		spaceMapPageID = storage.PageID(nextPageID)
	}
	return newStorageError("corrupted space map page")
}

func (db *DB) removeOwnedDataPageFromPhysicalStorageState(table *executor.Table, dataPageID storage.PageID, staged map[storage.PageID]stagedPage, order *[]storage.PageID) ([]storage.PageID, error) {
	if db == nil || table == nil {
		return nil, ErrInvalidArgument
	}
	if table.TableHeaderPageID() == 0 || table.FirstSpaceMapPageID() == 0 || dataPageID == 0 {
		return nil, newStorageError("corrupted header page")
	}

	headerPage, err := db.mutablePageImage(staged, order, table.TableHeaderPageID())
	if err != nil {
		return nil, err
	}
	if err := storage.ValidateTableHeaderPage(headerPage); err != nil {
		return nil, wrapStorageError(err)
	}

	var prevSpaceMapPageID storage.PageID
	currentSpaceMapPageID := table.FirstSpaceMapPageID()
	for currentSpaceMapPageID != 0 {
		spaceMapPage, err := db.mutablePageImage(staged, order, currentSpaceMapPageID)
		if err != nil {
			return nil, err
		}
		if err := storage.ValidateSpaceMapPage(spaceMapPage); err != nil {
			return nil, wrapStorageError(err)
		}
		owningTableID, err := storage.SpaceMapOwningTableID(spaceMapPage)
		if err != nil {
			return nil, wrapStorageError(err)
		}
		if owningTableID != table.TableID {
			return nil, newStorageError("corrupted space map page")
		}
		entryCount, err := storage.SpaceMapEntryCount(spaceMapPage)
		if err != nil {
			return nil, wrapStorageError(err)
		}
		for entryID := 0; entryID < entryCount; entryID++ {
			entry, err := storage.SpaceMapPageEntry(spaceMapPage, entryID)
			if err != nil {
				return nil, wrapStorageError(err)
			}
			if entry.DataPageID != dataPageID {
				continue
			}
			if err := storage.RemoveSpaceMapEntry(spaceMapPage, entryID); err != nil {
				return nil, wrapStorageError(err)
			}
			newOwnedDataCount := table.OwnedDataPageCount() - 1
			if err := storage.SetTableHeaderOwnedDataPageCount(headerPage, newOwnedDataCount); err != nil {
				return nil, wrapStorageError(err)
			}
			table.SetPhysicalTableRootMeta(table.TableHeaderPageID(), table.TableStorageFormatVersion(), table.FirstSpaceMapPageID(), newOwnedDataCount, table.OwnedSpaceMapPageCount())

			reclaimed := []storage.PageID{dataPageID}
			newEntryCount, err := storage.SpaceMapEntryCount(spaceMapPage)
			if err != nil {
				return nil, wrapStorageError(err)
			}
			if newEntryCount == 0 {
				nextPageID, err := storage.SpaceMapNextPageID(spaceMapPage)
				if err != nil {
					return nil, wrapStorageError(err)
				}
				if prevSpaceMapPageID == 0 {
					if err := storage.SetTableHeaderFirstSpaceMapPageID(headerPage, nextPageID); err != nil {
						return nil, wrapStorageError(err)
					}
					newFirst := storage.PageID(nextPageID)
					newOwnedSpaceMapCount := table.OwnedSpaceMapPageCount() - 1
					table.SetPhysicalTableRootMeta(table.TableHeaderPageID(), table.TableStorageFormatVersion(), newFirst, table.OwnedDataPageCount(), newOwnedSpaceMapCount)
					if err := storage.SetTableHeaderOwnedSpaceMapPageCount(headerPage, newOwnedSpaceMapCount); err != nil {
						return nil, wrapStorageError(err)
					}
				} else {
					prevPage, err := db.mutablePageImage(staged, order, prevSpaceMapPageID)
					if err != nil {
						return nil, err
					}
					if err := storage.SetSpaceMapNextPageID(prevPage, nextPageID); err != nil {
						return nil, wrapStorageError(err)
					}
					newOwnedSpaceMapCount := table.OwnedSpaceMapPageCount() - 1
					table.SetPhysicalTableRootMeta(table.TableHeaderPageID(), table.TableStorageFormatVersion(), table.FirstSpaceMapPageID(), table.OwnedDataPageCount(), newOwnedSpaceMapCount)
					if err := storage.SetTableHeaderOwnedSpaceMapPageCount(headerPage, newOwnedSpaceMapCount); err != nil {
						return nil, wrapStorageError(err)
					}
				}
				reclaimed = append(reclaimed, currentSpaceMapPageID)
			}
			return reclaimed, nil
		}
		nextPageID, err := storage.SpaceMapNextPageID(spaceMapPage)
		if err != nil {
			return nil, wrapStorageError(err)
		}
		prevSpaceMapPageID = currentSpaceMapPageID
		currentSpaceMapPageID = storage.PageID(nextPageID)
	}
	return nil, newStorageError("corrupted space map page")
}
