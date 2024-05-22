package server

import (
	"net"

	"github.com/haroldleong/easylive/conn"
	"github.com/haroldleong/easylive/processor"
	log "github.com/sirupsen/logrus"
)

type RtmpServer struct {
}

func New() *RtmpServer {
	return &RtmpServer{}
}

func (rs *RtmpServer) StartServe() (err error) {
	addr := ":1936"
	var rtmpListener net.Listener
	if rtmpListener, err = net.Listen("tcp", addr); err != nil {
		return err
	}
	log.Infof("rtmp server start.listening on:%s", addr)
	for {
		var netConn net.Conn
		if netConn, err = rtmpListener.Accept(); err != nil {
			return err
		}
		go func(netConn net.Conn) {
			rtmpConn := conn.NewRTMPConn(netConn)
			rtmpProcessor := processor.NewRTMPProcessor(rtmpConn)
			if err := rtmpProcessor.Start(); err != nil {
				log.Errorf("StartServe fail.err:%s", err)
			}
		}(netConn)
	}
}
