package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	rsp := &kvrpcpb.RawGetResponse{}
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		rsp.Error = err.Error()
	}
	if val == nil {
		rsp.NotFound = true
	}
	rsp.Value = val
	return rsp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	rsp := &kvrpcpb.RawPutResponse{}
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})
	if err != nil {
		rsp.Error = err.Error()
	}
	return rsp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	rsp := &kvrpcpb.RawDeleteResponse{}
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	})
	if err != nil {
		rsp.Error = err.Error()
	}
	return rsp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	rsp := &kvrpcpb.RawScanResponse{}
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	iterator := reader.IterCF(req.Cf)
	defer iterator.Close()

	iterator.Seek(req.StartKey)
	for i := uint32(0); iterator.Valid() && i < req.Limit; i++ {
		kvpair := &kvrpcpb.KvPair{}
		kvpair.Key = iterator.Item().Key()
		kvpair.Value, _ = iterator.Item().Value()
		rsp.Kvs = append(rsp.Kvs, kvpair)
		iterator.Next()
	}
	return rsp, nil
}
