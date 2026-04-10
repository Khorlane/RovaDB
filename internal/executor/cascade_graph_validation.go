package executor

import (
	"fmt"
	"slices"
	"strings"

	"github.com/Khorlane/RovaDB/internal/storage"
)

type cascadeGraphNode struct {
	tableID   uint32
	tableName string
}

type cascadeGraphEdge struct {
	parentTableID   uint32
	parentTableName string
	childTableID    uint32
	childTableName  string
	constraintName  string
}

func (e cascadeGraphEdge) constraintLabel() string {
	return fmt.Sprintf("%s.%s", e.childTableName, e.constraintName)
}

// ValidateCascadeGraphLegality rejects schema-wide illegal CASCADE graphs.
func ValidateCascadeGraphLegality(tables map[string]*Table) error {
	nodes, adjacency := buildCascadeGraph(tables)
	if err := validateCascadeCycleLegality(nodes, adjacency); err != nil {
		return err
	}
	if err := validateCascadePathLegality(nodes, adjacency); err != nil {
		return err
	}
	return nil
}

func buildCascadeGraph(tables map[string]*Table) ([]cascadeGraphNode, map[uint32][]cascadeGraphEdge) {
	nodes := make([]cascadeGraphNode, 0, len(tables))
	adjacency := make(map[uint32][]cascadeGraphEdge, len(tables))
	for _, table := range tables {
		if table == nil || table.TableID == 0 {
			continue
		}
		nodes = append(nodes, cascadeGraphNode{tableID: table.TableID, tableName: table.Name})
		for _, fk := range table.ForeignKeyDefs {
			if fk.OnDeleteAction != storage.CatalogForeignKeyDeleteActionCascade {
				continue
			}
			adjacency[fk.ParentTableID] = append(adjacency[fk.ParentTableID], cascadeGraphEdge{
				parentTableID:   fk.ParentTableID,
				parentTableName: tableNameByID(tables, fk.ParentTableID),
				childTableID:    fk.ChildTableID,
				childTableName:  table.Name,
				constraintName:  fk.Name,
			})
		}
	}
	slices.SortFunc(nodes, func(left, right cascadeGraphNode) int {
		if cmp := strings.Compare(left.tableName, right.tableName); cmp != 0 {
			return cmp
		}
		switch {
		case left.tableID < right.tableID:
			return -1
		case left.tableID > right.tableID:
			return 1
		default:
			return 0
		}
	})
	for tableID := range adjacency {
		slices.SortFunc(adjacency[tableID], func(left, right cascadeGraphEdge) int {
			if cmp := strings.Compare(left.childTableName, right.childTableName); cmp != 0 {
				return cmp
			}
			if cmp := strings.Compare(left.constraintName, right.constraintName); cmp != 0 {
				return cmp
			}
			switch {
			case left.childTableID < right.childTableID:
				return -1
			case left.childTableID > right.childTableID:
				return 1
			default:
				return 0
			}
		})
	}
	return nodes, adjacency
}

func validateCascadeCycleLegality(nodes []cascadeGraphNode, adjacency map[uint32][]cascadeGraphEdge) error {
	const (
		nodeStateUnvisited = 0
		nodeStateVisiting  = 1
		nodeStateDone      = 2
	)

	state := make(map[uint32]int, len(nodes))
	stackIndex := make(map[uint32]int, len(nodes))
	stackNodes := make([]uint32, 0, len(nodes))
	stackEdges := make([]cascadeGraphEdge, 0, len(nodes))

	var visit func(tableID uint32) []cascadeGraphEdge
	visit = func(tableID uint32) []cascadeGraphEdge {
		state[tableID] = nodeStateVisiting
		stackIndex[tableID] = len(stackNodes)
		stackNodes = append(stackNodes, tableID)

		for _, edge := range adjacency[tableID] {
			switch state[edge.childTableID] {
			case nodeStateUnvisited:
				stackEdges = append(stackEdges, edge)
				if cycle := visit(edge.childTableID); len(cycle) != 0 {
					return cycle
				}
				stackEdges = stackEdges[:len(stackEdges)-1]
			case nodeStateVisiting:
				start := stackIndex[edge.childTableID]
				cycle := append([]cascadeGraphEdge(nil), stackEdges[start:]...)
				cycle = append(cycle, edge)
				return cycle
			}
		}

		delete(stackIndex, tableID)
		stackNodes = stackNodes[:len(stackNodes)-1]
		state[tableID] = nodeStateDone
		return nil
	}

	for _, node := range nodes {
		if state[node.tableID] != nodeStateUnvisited {
			continue
		}
		if cycle := visit(node.tableID); len(cycle) != 0 {
			return newExecError(fmt.Sprintf("foreign key cascade cycle detected: constraints=%s", joinConstraintLabels(cycle)))
		}
	}
	return nil
}

func validateCascadePathLegality(nodes []cascadeGraphNode, adjacency map[uint32][]cascadeGraphEdge) error {
	tableNames := make(map[uint32]string, len(nodes))
	for _, node := range nodes {
		tableNames[node.tableID] = node.tableName
	}

	type pathConflict struct {
		sourceTableID uint32
		targetTableID uint32
		firstPath     []cascadeGraphEdge
		secondPath    []cascadeGraphEdge
	}

	appendPath := func(path []cascadeGraphEdge, edge cascadeGraphEdge) []cascadeGraphEdge {
		next := append([]cascadeGraphEdge(nil), path...)
		next = append(next, edge)
		return next
	}

	for _, source := range nodes {
		firstPaths := make(map[uint32][]cascadeGraphEdge)

		var visit func(tableID uint32, path []cascadeGraphEdge) *pathConflict
		visit = func(tableID uint32, path []cascadeGraphEdge) *pathConflict {
			for _, edge := range adjacency[tableID] {
				nextPath := appendPath(path, edge)
				if first, exists := firstPaths[edge.childTableID]; exists {
					return &pathConflict{
						sourceTableID: source.tableID,
						targetTableID: edge.childTableID,
						firstPath:     first,
						secondPath:    nextPath,
					}
				}
				firstPaths[edge.childTableID] = nextPath
				if conflict := visit(edge.childTableID, nextPath); conflict != nil {
					return conflict
				}
			}
			return nil
		}

		if conflict := visit(source.tableID, nil); conflict != nil {
			labels := uniqueConstraintLabels(conflict.firstPath, conflict.secondPath)
			return newExecError(fmt.Sprintf(
				"foreign key multiple cascade paths detected: source=%s target=%s constraints=%s",
				tableNames[conflict.sourceTableID],
				tableNames[conflict.targetTableID],
				strings.Join(labels, ","),
			))
		}
	}
	return nil
}

func joinConstraintLabels(edges []cascadeGraphEdge) string {
	labels := make([]string, 0, len(edges))
	for _, edge := range edges {
		labels = append(labels, edge.constraintLabel())
	}
	return strings.Join(labels, ",")
}

func uniqueConstraintLabels(paths ...[]cascadeGraphEdge) []string {
	labels := make([]string, 0)
	seen := make(map[string]struct{})
	for _, path := range paths {
		for _, edge := range path {
			label := edge.constraintLabel()
			if _, exists := seen[label]; exists {
				continue
			}
			seen[label] = struct{}{}
			labels = append(labels, label)
		}
	}
	return labels
}

func tableNameByID(tables map[string]*Table, tableID uint32) string {
	for _, table := range tables {
		if table != nil && table.TableID == tableID {
			return table.Name
		}
	}
	return ""
}
