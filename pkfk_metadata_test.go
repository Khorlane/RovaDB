package rovadb

import (
	"strings"
	"testing"

	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestPrimaryAndForeignKeyMetadataPersistAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, team_id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_teams_pk ON teams (id)"); err != nil {
		t.Fatalf("Exec(create teams pk index) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_users_pk ON users (id)"); err != nil {
		t.Fatalf("Exec(create users pk index) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_team ON users (team_id)"); err != nil {
		t.Fatalf("Exec(create users fk index) error = %v", err)
	}

	staged := cloneTables(db.tables)
	applyPrimaryKeyMetadata(t, staged["teams"], "pk_teams", "idx_teams_pk", "id")
	applyPrimaryKeyMetadata(t, staged["users"], "pk_users", "idx_users_pk", "id")
	applyForeignKeyMetadata(t, staged["users"], staged["teams"], "fk_users_team", "idx_users_team", storage.CatalogForeignKeyDeleteActionCascade, []string{"team_id"}, []string{"id"})
	if err := db.applyStagedCatalogOnly(staged); err != nil {
		t.Fatalf("applyStagedCatalogOnly() error = %v", err)
	}
	db.tables = staged
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	teamsPK := db.tables["teams"].PrimaryKeyDefinition()
	if teamsPK == nil || teamsPK.Name != "pk_teams" || teamsPK.IndexID == 0 || len(teamsPK.Columns) != 1 || teamsPK.Columns[0] != "id" {
		t.Fatalf("teams PrimaryKeyDefinition() = %#v, want pk_teams on id", teamsPK)
	}
	usersPK := db.tables["users"].PrimaryKeyDefinition()
	if usersPK == nil || usersPK.Name != "pk_users" {
		t.Fatalf("users PrimaryKeyDefinition() = %#v, want pk_users", usersPK)
	}
	usersFKs := db.tables["users"].ForeignKeyDefinitions()
	if len(usersFKs) != 1 {
		t.Fatalf("len(users ForeignKeyDefinitions()) = %d, want 1", len(usersFKs))
	}
	if usersFKs[0].OnDeleteAction != storage.CatalogForeignKeyDeleteActionCascade {
		t.Fatalf("users FK OnDeleteAction = %d, want %d", usersFKs[0].OnDeleteAction, storage.CatalogForeignKeyDeleteActionCascade)
	}
	if got := executor.ForeignKeysReferencingTable(db.tables, db.tables["teams"].TableID); len(got) != 1 || got[0].Name != "fk_users_team" {
		t.Fatalf("ForeignKeysReferencingTable() = %#v, want fk_users_team", got)
	}
	if !executor.IndexReferencedByConstraint(db.tables, db.tables["users"].IndexDefinition("idx_users_team").IndexID) {
		t.Fatal("IndexReferencedByConstraint(idx_users_team) = false, want true")
	}
}

func TestOpenRejectsForeignKeyDependingOnMissingParentPrimaryKey(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE teams (id INT)"); err != nil {
		t.Fatalf("Exec(create teams) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, team_id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX idx_teams_pk ON teams (id)"); err != nil {
		t.Fatalf("Exec(create teams pk index) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX idx_users_team ON users (team_id)"); err != nil {
		t.Fatalf("Exec(create users fk index) error = %v", err)
	}

	staged := cloneTables(db.tables)
	applyPrimaryKeyMetadata(t, staged["teams"], "pk_teams", "idx_teams_pk", "id")
	applyForeignKeyMetadata(t, staged["users"], staged["teams"], "fk_users_team", "idx_users_team", storage.CatalogForeignKeyDeleteActionRestrict, []string{"team_id"}, []string{"id"})
	if err := db.applyStagedCatalogOnly(staged); err != nil {
		t.Fatalf("applyStagedCatalogOnly(valid) error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawDB, pager := openRawStorage(t, path)
	catalog, err := storage.LoadCatalog(pager)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	catalog = catalogWithDirectoryRootsForSave(t, rawDB.File(), catalog)
	for i := range catalog.Tables {
		if catalog.Tables[i].Name != "users" || len(catalog.Tables[i].ForeignKeys) == 0 {
			continue
		}
		catalog.Tables[i].ForeignKeys[0].ParentPrimaryKeyName = "missing_pk"
	}
	if err := storage.SaveCatalog(pager, catalog); err != nil {
		t.Fatalf("SaveCatalog() error = %v", err)
	}
	rewriteDirectoryRootMappingsForCatalogTables(t, rawDB.File(), catalog)
	if err := pager.FlushDirty(); err != nil {
		t.Fatalf("pager.FlushDirty() error = %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("rawDB.Close() error = %v", err)
	}

	db, err = Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("reopen Open() error = nil, want missing parent PK dependency rejection")
	}
	if !strings.Contains(err.Error(), "constraint/table mismatch") {
		t.Fatalf("reopen Open() error = %v, want constraint/table mismatch", err)
	}
}

func applyPrimaryKeyMetadata(t *testing.T, table *executor.Table, constraintName, indexName string, columns ...string) {
	t.Helper()
	if table == nil {
		t.Fatal("applyPrimaryKeyMetadata() requires table")
	}
	indexDef := table.IndexDefinition(indexName)
	if indexDef == nil {
		t.Fatalf("IndexDefinition(%q) = nil", indexName)
	}
	table.PrimaryKeyDef = &storage.CatalogPrimaryKey{
		Name:       constraintName,
		TableID:    table.TableID,
		Columns:    append([]string(nil), columns...),
		IndexID:    indexDef.IndexID,
		ImplicitNN: true,
	}
}

func applyForeignKeyMetadata(t *testing.T, child *executor.Table, parent *executor.Table, constraintName, childIndexName string, onDelete uint8, childColumns, parentColumns []string) {
	t.Helper()
	if child == nil || parent == nil || parent.PrimaryKeyDef == nil {
		t.Fatal("applyForeignKeyMetadata() requires child, parent, and parent PK")
	}
	indexDef := child.IndexDefinition(childIndexName)
	if indexDef == nil {
		t.Fatalf("IndexDefinition(%q) = nil", childIndexName)
	}
	child.ForeignKeyDefs = append(child.ForeignKeyDefs, storage.CatalogForeignKey{
		Name:                 constraintName,
		ChildTableID:         child.TableID,
		ChildColumns:         append([]string(nil), childColumns...),
		ParentTableID:        parent.TableID,
		ParentColumns:        append([]string(nil), parentColumns...),
		ParentPrimaryKeyName: parent.PrimaryKeyDef.Name,
		ChildIndexID:         indexDef.IndexID,
		OnDeleteAction:       onDelete,
	})
}
