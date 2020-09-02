package http

import (
	"github.com/transaction-wg/seata-golang/pkg/tm"
	"strings"
)

type tmEndPointConfig struct {
	tm.TransactionInfo
	BeginEndPoint string `json:"begin_end_point"`
}

type tmConfig struct {
	EndPointConfigs []*tmEndPointConfig `json:"end_point_configs"`
}

func (c *tmConfig) FindTMEndPointConfig(endpoint string) (*tmEndPointConfig,bool) {
	if c.EndPointConfigs != nil && len(c.EndPointConfigs) > 0 {
		for _,config := range c.EndPointConfigs {
			if strings.EqualFold(config.BeginEndPoint,endpoint) {
				return config,true
			}
		}
	}
	return nil,false
}
