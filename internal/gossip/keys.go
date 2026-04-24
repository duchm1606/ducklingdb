package gossip

import "fmt"

const (
	KeyNodeDescPrefix     = "node-desc:"
	KeyStoreDescPrefix    = "store-desc:"
	KeyNodeLivenessPrefix = "node-liveness:"
)

func MakeNodeDescKey(nodeID int32) string {
	return fmt.Sprintf("%s%d", KeyNodeDescPrefix, nodeID)
}

func MakeStoreDescKey(storeID int32) string {
	return fmt.Sprintf("%s%d", KeyStoreDescPrefix, storeID)
}

func MakeNodeLivenessKey(nodeID int32) string {
	return fmt.Sprintf("%s%d", KeyNodeLivenessPrefix, nodeID)
}
