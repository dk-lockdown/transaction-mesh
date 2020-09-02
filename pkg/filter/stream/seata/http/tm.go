package http

import (
	"context"
	"fmt"
	"net/http"
	"sync"
)

import (
	"mosn.io/api"
	mosnctx "mosn.io/mosn/pkg/context"
	"mosn.io/mosn/pkg/protocol"
	"mosn.io/mosn/pkg/types"
	"mosn.io/pkg/buffer"

	context2 "github.com/transaction-wg/seata-golang/pkg/context"
	"github.com/transaction-wg/seata-golang/pkg/tm"
)

const SEATA_XID = "SEATA_XID"
const GLOBAL_TRANSACTION types.ContextKey = -10000

const (
	GlobalTransactionBeginFailed    api.ResponseFlag = 0x10001
	GlobalTransactionCommitFailed   api.ResponseFlag = 0x10002
	GlobalTransactionRollbackFailed api.ResponseFlag = 0x10003
)

type tmStreamFilter struct {
	receiveHandler api.StreamReceiverFilterHandler
	sendHandler    api.StreamSenderFilterHandler
	config         *tmConfig
	txMap          *sync.Map
}

func (f *tmStreamFilter) SetReceiveFilterHandler(handler api.StreamReceiverFilterHandler) {
	f.receiveHandler = handler
}

func (f *tmStreamFilter) OnReceive(ctx context.Context, headers api.HeaderMap, buf buffer.IoBuffer, trailers api.HeaderMap) api.StreamFilterStatus {
	if protocol.HTTP1 == f.receiveHandler.RequestInfo().Protocol() {
		requestPath, ok := headers.Get(protocol.MosnHeaderPathKey)
		if ok {
			txConf, found := f.config.FindTMEndPointConfig(requestPath)
			if found {
				xid, exists := headers.Get(SEATA_XID)
				rootCtx := context2.NewRootContext(ctx)
				if exists {
					rootCtx.Bind(xid)
				}
				tx := tm.GetCurrentOrCreate(rootCtx)

				switch txConf.Propagation {
				case tm.REQUIRED:
					break
				case tm.REQUIRES_NEW:
					if exists {
						rootCtx.Unbind()
					}
					break
				case tm.NOT_SUPPORTED:
					if exists {
						rootCtx.Unbind()
						headers.Del(SEATA_XID)
						f.receiveHandler.SetRequestHeaders(headers)
					}
					return api.StreamFilterContinue
				case tm.SUPPORTS:
					// if xid not exists, not begin global transaction
					if !exists {
						return api.StreamFilterContinue
					}
					break
				case tm.NEVER:
					if exists {
						f.receiveHandler.SendHijackReplyWithBody(http.StatusExpectationFailed, headers,
							fmt.Sprintf("Existing transaction found for transaction marked with propagation 'never',xid = %s", xid))
						return api.StreamFilterStop
					} else {
						return api.StreamFilterContinue
					}
				case tm.MANDATORY:
					if !exists {
						f.receiveHandler.SendHijackReplyWithBody(http.StatusPreconditionFailed, headers,
							"No existing transaction found for transaction marked with propagation 'mandatory'")
						return api.StreamFilterStop
					}
					break
				default:
					f.receiveHandler.SendHijackReplyWithBody(http.StatusNotImplemented, headers,
						fmt.Sprintf("Not Supported Propagation: %s", txConf.Propagation.String()))
					return api.StreamFilterStop
				}

				// begin global transaction
				beginErr := tx.BeginWithTimeoutAndName(txConf.TimeOut, txConf.Name, rootCtx)
				if beginErr != nil {
					f.receiveHandler.RequestInfo().SetResponseFlag(GlobalTransactionBeginFailed)
					f.receiveHandler.SendHijackReplyWithBody(http.StatusInternalServerError, headers, beginErr.Error())
					return api.StreamFilterStop
				}

				streamId := mosnctx.Get(ctx, types.ContextKeyStreamID)
				requestId := streamId.(uint64)
				f.txMap.Store(requestId, tx)
				return api.StreamFilterContinue
			}
		}
	}
	return api.StreamFilterContinue
}

func (f *tmStreamFilter) SetSenderFilterHandler(handler api.StreamSenderFilterHandler) {
	f.sendHandler = handler
}

func (f *tmStreamFilter) Append(ctx context.Context, headers api.HeaderMap, buf buffer.IoBuffer, trailers api.HeaderMap) api.StreamFilterStatus {
	if protocol.HTTP1 == f.sendHandler.RequestInfo().Protocol() {
		downStreamHeader := ctx.Value(types.ContextKeyDownStreamHeaders)
		requestHeader := downStreamHeader.(api.HeaderMap)
		requestPath, ok := requestHeader.Get(protocol.MosnHeaderPathKey)
		if ok {
			conf, found := f.config.FindTMEndPointConfig(requestPath)
			if found {
				xid, exists := requestHeader.Get(SEATA_XID)
				rootCtx := context2.NewRootContext(ctx)
				if exists {
					rootCtx.Bind(xid)
				}

				switch conf.Propagation {
				case tm.REQUIRED:
				case tm.REQUIRES_NEW:
					break
				case tm.NOT_SUPPORTED:
					return api.StreamFilterContinue
				case tm.SUPPORTS:
					if !exists {
						return api.StreamFilterContinue
					}
					break
				case tm.NEVER:
					return api.StreamFilterContinue
				case tm.MANDATORY:
					break
				default:
					break
				}

				streamId := mosnctx.Get(ctx, types.ContextKeyStreamID)
				requestId := streamId.(uint64)
				globalTransaction, loaded := f.txMap.Load(requestId)
				if loaded {
					tx := globalTransaction.(tm.GlobalTransaction)

					if f.sendHandler.RequestInfo().ResponseCode() == http.StatusOK {
						commitErr := tx.Commit(rootCtx)
						if commitErr != nil {
							f.receiveHandler.RequestInfo().SetResponseFlag(GlobalTransactionCommitFailed)
							f.receiveHandler.SendHijackReplyWithBody(http.StatusInternalServerError, headers, commitErr.Error())
							f.txMap.Delete(requestId)
							return api.StreamFilterStop
						}
					} else {
						rollbackErr := tx.Rollback(rootCtx)
						if rollbackErr != nil {
							f.receiveHandler.RequestInfo().SetResponseFlag(GlobalTransactionRollbackFailed)
							f.receiveHandler.SendHijackReplyWithBody(http.StatusInternalServerError, headers, rollbackErr.Error())
							f.txMap.Delete(requestId)
							return api.StreamFilterStop
						}
					}

					f.txMap.Delete(requestId)
				}

				return api.StreamFilterContinue
			}
		}
	}
	return api.StreamFilterContinue
}

func (f *tmStreamFilter) OnDestroy() {

}
