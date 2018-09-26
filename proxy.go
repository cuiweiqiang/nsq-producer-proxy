/*
* @Author: cuiweiqiang
* @Date:   2018-09-10 12:37:09
* @Last Modified by:   cuiweiqiang
* @Last Modified time: 2018-09-26 11:00:35
 */

package proxy

import (
	"encoding/json"
	"errors"
	"fmt"

	nsq "github.com/nsqio/go-nsq"
)

var Retry = 1

type Producers struct {
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	Topics           []string `json:"topics"`

	valid bool
	inuse bool
}

type Data struct {
	Producers []Producers `json:"producers"`
}

type response struct {
	StatusCode int    `json:"status_code"`
	StatusText string `json:"status_text"`
	Data       Data   `json:"data"`
}

type IProducer interface {
	Stop()
	Publish(topic string, message []byte) error
	SetLogger(l logger, level nsq.LogLevel)
}

type logger interface {
	Output(calldepth int, s string) error
}

type Producer struct {
	lookupds   []string
	config     *nsq.Config
	validNodes []Producers

	current   *nsq.Producer
	inuseNode *Producers

	level nsq.LogLevel
	log   logger
}

func (p *Producer) Stop() {
	if p.current != nil {
		p.current.Stop()
	}

	p.current = nil
	p.inuseNode = nil
	p.validNodes = nil
}

func (p *Producer) Publish(topic string, body []byte) error {
	producer, err := p.Retry()
	if nil != err {
		return err
	}

	err = producer.Publish(topic, body)
	if nil != err {
		producer, err = p.Retry()
		if nil != err {
			return err
		}

		return producer.Publish(topic, body)
	}

	return nil
}

func (p *Producer) SetLogger(l logger, level nsq.LogLevel) {
	p.log = l
	p.level = level
}

func (p *Producer) Retry() (*nsq.Producer, error) {
	producer, err := p.getProducer()
	if err != nil {
		p.Stop()
		for i := 0; i < Retry; i++ {
			p.validNodes, err = pickNsqds(p.lookupds)
			if nil != err {
				continue
			}
			producer, err = p.getProducer()
			if nil != err {
				break
			}
		}
		if nil != err {
			return nil, err
		}
	}

	return producer, err
}

func (p *Producer) getProducer() (*nsq.Producer, error) {
	if p.current != nil {
		if err := p.current.Ping(); err == nil {
			return p.current, nil
		}

		p.inuseNode.inuse = false
		p.inuseNode = nil
		p.current.Stop()
		p.current = nil
	}

	if p.validNodes != nil ||
		len(p.validNodes) == 0 {
		return nil, errors.New("No valid node")
	}

	var producer *nsq.Producer

	var err error

	for index, v := range p.validNodes {
		if !v.valid {
			continue
		}

		producer, err = nsq.NewProducer(fmt.Sprintf("%s:%d", v.BroadcastAddress, v.TCPPort), p.config)
		if err != nil {
			continue
		}

		if p.log != nil {
			producer.SetLogger(p.log, p.level)
		}

		err = producer.Ping()
		if err != nil {
			continue
		}

		p.validNodes[index].inuse = true
		p.inuseNode = &p.validNodes[index]
		p.current = producer
		break
	}

	return producer, err

}

func pickNsqd(addr string) ([]Producers, error) {
	body, err := Get("http://"+addr+"/nodes", nil)

	if err != nil {
		return nil, err
	}

	var res response

	if err = json.Unmarshal(body, &res); err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, errors.New("Pick Nsqd Failed, Please checkin. ")
	}

	return res.Data.Producers, nil
}

func pickNsqds(lookupds []string) ([]Producers, error) {
	if lookupds == nil || len(lookupds) == 0 {
		return nil, errors.New("lookupd str must not be nil")
	}

	var nodes []Producers
	var err error
	for _, v := range lookupds {
		nodes, err = pickNsqd(v)

		if err != nil {
			continue
		}

		return nodes, nil
	}
	return nil, nil
}

func NewProducer(nsqlookupds []string, config *nsq.Config, log logger, level nsq.LogLevel) (IProducer, error) {
	producer := &Producer{
		lookupds: nsqlookupds,
		config:   config,
	}

	producers, err := pickNsqds(nsqlookupds)
	if err != nil {
		return nil, err
	}

	producer.validNodes = producers
	producer.log = log
	producer.level = level

	_, err = producer.getProducer()

	if err != nil {
		return nil, err
	}

	return producer, nil
}
