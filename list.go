package client

import (
	"fmt"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

// list a directory
func (clt *EtcdHRCHYClient) List(key string) ([]*Node, error) {
	key, _, err := clt.ensureKey(key)
	if err != nil {
		return nil, err
	}
	// directory start with /
	dir := key + "/"

	txn := clt.client.Txn(clt.ctx)
	// make sure the list key is a directory
	txn.If(
		clientv3.Compare(
			clientv3.Value(key),
			"=",
			clt.dirValue,
		),
	).Then(
		clientv3.OpGet(dir, clientv3.WithPrefix()),
	)

	txnResp, err := txn.Commit()
	if err != nil {
		return nil, err
	}

	if !txnResp.Succeeded {
		return nil, ErrorListKey
	} else {
		if len(txnResp.Responses) > 0 {
			rangeResp := txnResp.Responses[0].GetResponseRange()
			return clt.list(dir, rangeResp.Kvs)
		} else {
			// empty directory
			return []*Node{}, nil
		}
	}
}

func (clt *EtcdHRCHYClient) checkSubDir(dir string, name string) (firstDir string, err error) {
	subnames := strings.Split(name, "/")

	key := strings.TrimSuffix(dir, "/")
	for idx, d := range subnames[:len(subnames)-1] {
		txn := clt.client.Txn(clt.ctx)

		key = key + "/" + d

		txn.If(
			clientv3.Compare(
				clientv3.Version(key),
				"=",
				0,
			),
		).Then(
			clientv3.OpPut(key, clt.dirValue),
		)

		txnResp, err := txn.Commit()
		if err != nil {
			return "", err
		}

		if idx == 0 {
			firstDir = d
		}

		if !txnResp.Succeeded {
			//TODO: do nothing now
		}
	}
	return firstDir, nil
}

// pick key/value under the dir
func (clt *EtcdHRCHYClient) list(dir string, kvs []*mvccpb.KeyValue) ([]*Node, error) {
	nodes := []*Node{}

	names := []string{}

	var err error
	var node *Node

	nameContained := func(names []string, key string) bool {
		for _, n := range names {
			if n == key {
				return true
			}
		}
		return false
	}
	for _, kv := range kvs {
		name := strings.TrimPrefix(string(kv.Key), dir)
		if strings.Contains(name, "/") {
			// secondary directory
			name, err = clt.checkSubDir(dir, name)
			if err != nil {
				fmt.Println("list error:", err)
				continue
			}

			if nameContained(names, name) {
				continue
			}

			node, err = clt.Get(strings.TrimPrefix(dir+name, clt.rootKey))
			if err != nil {
				fmt.Println("list get node error:", err)
				continue
			}
		} else {
			if nameContained(names, name) {
				continue
			}
			node = clt.createNode(kv)
		}

		names = append(names, name)
		nodes = append(nodes, node)
	}
	return nodes, nil
}
