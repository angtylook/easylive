package conn

import (
	"bufio"
	"fmt"
	"io"
	"net"

	log "github.com/sirupsen/logrus"

	"github.com/Monibuca/engine/v2/util/bits/pio"
)

const (
	remoteWindowAckSize = 5000000 // 客户端可接受的最大数据包的值
)

// 连接类型，推流或者拉流
type ConnectionType int32

const (
	ConnectionTypePublish ConnectionType = iota // 推流
	ConnectionTypePull                          // 拉流
)

// RTMPConn rtmp连接，处理chunk
type RTMPConn struct {
	NetConn   net.Conn
	bufReader *bufio.Reader // 读数据io
	bufWriter *bufio.Writer // 写数据io
	csidMap   map[uint32]*ChunkStream

	writeMaxChunkSize   int
	readMaxChunkSize    int    // chunk size
	remoteWindowAckSize uint32 // ack size

	tmpReadData  []byte // 用于临时读
	tmpWriteData []byte // 用于临时写

	received    uint32
	ackReceived uint32

	messageDone bool // 是否处理完

	ConnInfo *ConnectInfo // 连接信息

	ConnType ConnectionType // 是否是推流or拉流
}

func (c *RTMPConn) MessageDone() bool {
	return c.messageDone
}

func NewRTMPConn(netConn net.Conn) *RTMPConn {
	conn := &RTMPConn{
		writeMaxChunkSize: 128,
		readMaxChunkSize:  128,
	}
	conn.NetConn = netConn
	conn.bufWriter = bufio.NewWriterSize(netConn, pio.RecommendBufioSize)
	conn.bufReader = bufio.NewReaderSize(netConn, pio.RecommendBufioSize)
	conn.csidMap = make(map[uint32]*ChunkStream)

	conn.tmpWriteData = make([]byte, 4096)
	conn.tmpReadData = make([]byte, 4096)

	conn.ConnInfo = &ConnectInfo{}
	conn.remoteWindowAckSize = remoteWindowAckSize
	return conn
}

func (c *RTMPConn) HandshakeServer() error {
	var random [(1 + 1536*2) * 2]byte

	C0C1C2 := random[:1536*2+1]
	C0 := C0C1C2[:1]
	// C1: time + zero + random
	// or
	//time: 4bytes
	//version: 4bytes
	//digest: 764bytes
	//key: 764bytes
	C1 := C0C1C2[1 : 1536+1]
	C0C1 := C0C1C2[:1536+1]
	C2 := C0C1C2[1536+1:]

	S0S1S2 := random[1536*2+1:]
	S0 := S0S1S2[:1]
	S1 := S0S1S2[1 : 1536+1]
	S0S1 := S0S1S2[:1536+1]
	S2 := S0S1S2[1536+1:]

	// < C0C1
	if _, err := io.ReadFull(c.bufReader, C0C1); err != nil {
		return err
	}
	if C0[0] != 3 {
		return fmt.Errorf("rtmp: handshake version=%d invalid", C0[0])
	}

	// 版本号，固定为0x03
	S0[0] = 3

	clientTime := pio.U32BE(C1[0:4])
	serverTime := clientTime
	// 4 字节的程序版本：C1 一般是 0x80000702，S1 是 0x04050001
	serverVersion := uint32(0x04050001)
	clientVersion := pio.U32BE(C1[4:8])

	// 判断是简单握手还是复杂握手
	if clientVersion != 0 {
		log.Infof("use complex handshake")
		var ok bool
		var digest []byte
		if ok, digest = hsParseC1(C1, hsClientPartialKey, hsServerFullKey); !ok {
			return fmt.Errorf("rtmp: handshake server: C1 invalid")
		}
		hsCreateS01(S0S1, serverTime, serverVersion, hsServerPartialKey)
		hsCreateS2(S2, digest)
	} else {
		log.Infof("use simple handshake")
		copy(S1, C1)
		copy(S2, C2)
	}

	// > S0S1S2
	if _, err := c.bufWriter.Write(S0S1S2); err != nil {
		return err
	}
	if err := c.bufWriter.Flush(); err != nil {
		return err
	}

	// < C2
	if _, err := io.ReadFull(c.bufReader, C2); err != nil {
		return err
	}
	return nil
}

func (c *RTMPConn) ReadChunk() (*ChunkStream, error) {
	// 读取basic header
	var (
		data []byte
		err  error
	)
	if data, err = c.readData(1); err != nil {
		return nil, err
	}
	header := data[0]
	format := header >> 6
	csid := uint32(header) & 0x3f
	if csid, err = c.getRealCSID(csid); err != nil {
		return nil, err
	}
	cs, ok := c.csidMap[csid]
	if !ok {
		cs = &ChunkStream{
			CSID: csid,
		}
		c.csidMap[cs.CSID] = cs
	}
	// 读取message header https://github.com/AlexWoo/doc/blob/master/Media/RTMP%20Chunk%20Header.md
	switch format {
	case 0: //	0: Message Header 为 11 字节编码，完整的header，处于流的开头
		var mh []byte
		if mh, err = c.readData(11); err != nil {
			return nil, err
		}
		cs.Format = format
		cs.Timestamp = pio.U24BE(mh[0:3])
		cs.Length = pio.U24BE(mh[3:6])
		cs.TypeID = uint32(mh[6])
		cs.StreamID = pio.U32LE(mh[7:11])
		if cs.Timestamp == 0xffffff {
			if data, err = c.readData(4); err != nil {
				return nil, err
			}
			cs.Timestamp = pio.U32BE(data)
			cs.useExtendTimeStamp = true
		} else {
			cs.useExtendTimeStamp = false
		}
		cs.initData()
	case 1: //1: Message Header 为 7 字节编码，通常在fmt0之后。针对可变大小的chunk
		var mh []byte
		if mh, err = c.readData(7); err != nil {
			return nil, err
		}
		cs.Format = format
		timestamp := pio.U24BE(mh[0:3])
		cs.Length = pio.U24BE(mh[3:6])
		cs.TypeID = uint32(mh[6])
		if timestamp == 0xffffff {
			if data, err = c.readData(4); err != nil {
				return nil, err
			}
			cs.Timestamp = pio.U32BE(data)
			cs.useExtendTimeStamp = true
		} else {
			cs.useExtendTimeStamp = false
		}
		cs.timeDelta = timestamp
		cs.Timestamp += timestamp
		cs.initData()
	case 2: //2: Message Header 为 3 字节编码，只有一个timestamp delta。针对固定大小
		var mh []byte
		if mh, err = c.readData(3); err != nil {
			return nil, err
		}
		cs.Format = format
		timestamp := pio.U24BE(mh[0:3])
		if timestamp == 0xffffff {
			if data, err = c.readData(4); err != nil {
				return nil, err
			}
			cs.Timestamp = pio.U32BE(data)
			cs.useExtendTimeStamp = true
		} else {
			cs.useExtendTimeStamp = false
		}
		cs.timeDelta = timestamp
		cs.Timestamp += timestamp
		cs.initData()
	case 3:
		//3: Message Header 为 0 字节编码
		//如果前面一个 chunk 里面存在 timestampDelta，那么计算 fmt 为 3 的 chunk 时，就直接相加，
		//如果没有，则是使用前一个 chunk 的 timestamp 来进行相加
		if cs.remain == 0 { //这个chunk都是fmt3类型，不为0需要读chunk剩余部分
			switch cs.Format {
			case 0:
				if cs.useExtendTimeStamp {
					if data, err = c.readData(4); err != nil {
						return nil, err
					}
					cs.Timestamp = pio.U32BE(data)
				}
			case 1, 2:
				var timestamp uint32
				if cs.useExtendTimeStamp {
					if data, err = c.readData(4); err != nil {
						return nil, err
					}
					timestamp = pio.U32BE(data)
				} else {
					timestamp = cs.timeDelta
				}
				cs.Timestamp += timestamp
			}
			cs.initData()
		}
	default:
		return nil, fmt.Errorf("invalid format=%d", format)
	}
	size := int(cs.remain)
	if size > c.readMaxChunkSize {
		size = c.readMaxChunkSize
	}

	buf := cs.Data[cs.index : cs.index+uint32(size)]
	if _, err = io.ReadFull(c.bufReader, buf); err != nil {
		return nil, err
	}

	cs.index += uint32(size)
	cs.remain -= uint32(size)
	if cs.remain == 0 {
		cs.finish = true
	}

	return cs, nil
}

func (c *RTMPConn) Ack(cs *ChunkStream) {
	c.received += cs.Length
	c.ackReceived += cs.Length
	// 处理溢出，acknowledge如果累积超过0xf0000000，就置零
	if c.received >= 0xf0000000 {
		c.received = 0
	}
	if c.ackReceived >= c.remoteWindowAckSize {
		log.Infof("Ack.true ack,ackReceived:%d", c.ackReceived)
		ackChunk := c.NewAck(c.ackReceived)
		c.writeChunk(&ackChunk, c.writeMaxChunkSize)
		c.ackReceived = 0
	}
}
