/*
 * Copyright 2011 Nan Deng
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package pushsrv

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/uniqush/pushsys"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"sync"
	"time"
)

type pushRequest struct {
	dp *DeliveryPoint
	notif *Notification
	mid uint32
	resChan chan<- *PushResult
}

type apnsPushService struct {
	nextid uint32
	conns  map[string]net.Conn
	connChan map[string]chan *pushRequest
	lock *sync.Mutex
	pfp    PushFailureHandler
}

func InstallAPNS() {
	GetPushServiceManager().RegisterPushServiceType(newAPNSPushService())
}

func newAPNSPushService() *apnsPushService {
	ret := new(apnsPushService)
	ret.conns = make(map[string]net.Conn, 5)
	ret.connChan = make(map[string]chan *pushRequest, 5)
	ret.lock = new(sync.Mutex)
	return ret
}

func (p *apnsPushService) Name() string {
	return "apns"
}

func (p *apnsPushService) SetAsyncFailureHandler(pfp PushFailureHandler) {
	p.pfp = pfp
}

func (p *apnsPushService) Finalize() {
	for _, c := range p.conns {
		c.Close()
	}
}
func (p *apnsPushService) BuildPushServiceProviderFromMap(kv map[string]string, psp *PushServiceProvider) error {
	if service, ok := kv["service"]; ok {
		psp.FixedData["service"] = service
	} else {
		return errors.New("NoService")
	}

	if cert, ok := kv["cert"]; ok && len(cert) > 0 {
		psp.FixedData["cert"] = cert
	} else {
		return errors.New("NoCertificate")
	}

	if key, ok := kv["key"]; ok && len(key) > 0 {
		psp.FixedData["key"] = key
	} else {
		return errors.New("NoPrivateKey")
	}

	if sandbox, ok := kv["sandbox"]; ok {
		if sandbox == "true" {
			psp.VolatileData["addr"] = "gateway.sandbox.push.apple.com:2195"
			return nil
		}
	}
	psp.VolatileData["addr"] = "gateway.push.apple.com:2195"
	return nil
}

func (p *apnsPushService) BuildDeliveryPointFromMap(kv map[string]string, dp *DeliveryPoint) error {
	if service, ok := kv["service"]; ok && len(service) > 0 {
		dp.FixedData["service"] = service
	} else {
		return errors.New("NoService")
	}
	if sub, ok := kv["subscriber"]; ok && len(sub) > 0 {
		dp.FixedData["subscriber"] = sub
	} else {
		return errors.New("NoSubscriber")
	}
	if devtoken, ok := kv["devtoken"]; ok && len(devtoken) > 0 {
		dp.FixedData["devtoken"] = devtoken
	} else {
		return errors.New("NoDevToken")
	}
	return nil
}

func toAPNSPayload(n *Notification) ([]byte, error) {
	payload := make(map[string]interface{})
	aps := make(map[string]interface{})
	alert := make(map[string]interface{})
	for k, v := range n.Data {
		switch k {
		case "msg":
			alert["body"] = v
		case "badge":
			b, err := strconv.Atoi(v)
			if err != nil {
				continue
			} else {
				aps["badge"] = b
			}
		case "sound":
			aps["sound"] = v
		case "img":
			alert["launch-image"] = v
		case "id":
			continue
		case "expiry":
			continue
		default:
			payload[k] = v
		}
	}
	aps["alert"] = alert
	payload["aps"] = aps
	j, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func writen(w io.Writer, buf []byte) error {
	n := len(buf)
	for n >= 0 {
		l, err := w.Write(buf)
		if err != nil {
			return err
		}
		if l >= n {
			return nil
		}
		n -= l
		buf = buf[l:]
	}
	return nil
}

func (p *apnsPushService) getConn(psp *PushServiceProvider) (net.Conn, error) {
	name := psp.Name()
	if conn, ok := p.conns[name]; ok {
		return conn, nil
	}
	return p.reconnect(psp)
}

func (p *apnsPushService) reconnect(psp *PushServiceProvider) (net.Conn, error) {
	name := psp.Name()
	if conn, ok := p.conns[name]; ok {
		conn.Close()
	}
	cert, err := tls.LoadX509KeyPair(psp.FixedData["cert"], psp.FixedData["key"])
	if err != nil {
		return nil, NewInvalidPushServiceProviderError(psp, err)
	}
	conf := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsconn, err := tls.Dial("tcp", psp.VolatileData["addr"], conf)
	if err != nil {
		return nil, NewConnectionError(psp, err, "DialErr")
	}
	err = tlsconn.Handshake()
	if err != nil {
		return nil, NewConnectionError(psp, err, "HandshakeErr")
	}
	p.conns[name] = tlsconn
	return tlsconn, nil
}

func (self *apnsPushService) singlePush(psp *PushServiceProvider, dp *DeliveryPoint, notif *Notification, mid uint32) {
	devtoken :=
}

func (self *apnsPushService) getRequestChannel(psp *PushServiceProvider) chan *pushRequest{
	var ch chan *pushRequest
	var ok bool
	self.lock.Lock()
	if ch, ok = self.connChan[psp.Name()]; !ok {
		ch = make(chan *pushRequest)
		self.connChan[psp.Name()] = ch
		go pushWorker(psp, ch)
	}
	self.lock.Unlock()
	return ch
}

type struct apnsResult {
	msgId uint32
	err error
}

func (self *apnsPushService) Push(psp *PushServiceProvider, dpQueue <-chan *DeliveryPoint, resQueue chan<- *PushResult, notif *Notification) {
	/* First, get the request channel */
	ch := self.getRequestChannel(psp)

	for dp := range dpQueue {
		go func () {
			resultChannel := make(chan *PushResult, 1)
			req := new(pushRequest)
			req.dp = dp
			req.notif = notif
			req.resChan = resultChannel
			res.mid := atomic.AddUint32(&(self.nextid), 1)

			ch<-req

			select {
			case res := <-resultChannel:
				resQueue <-
			case <-time.After(time.ParseDuration("5s")):
				return

			}

		}()
	}
}

func (p *apnsPushService) waitError(id string,
	c net.Conn,
	psp *PushServiceProvider,
	dp *DeliveryPoint,
	n *Notification) {
	duration, err := time.ParseDuration("5s")
	if err != nil {
		return
	}
	deadline := time.Now().Add(duration)
	//c.SetReadTimeout(5E9)
	err = c.SetDeadline(deadline)
	if err != nil {
		return
	}
	readb := [6]byte{}
	nr, err := c.Read(readb[:])
	if err != nil {
		return
	}
	if nr > 0 {
		switch readb[1] {
		case 2:
			p.pfp.OnPushFail(p,
				id,
				NewInvalidDeliveryPointError(psp,
					dp,
					errors.New("Missing device token")))
		case 3:
			err := NewInvalidNotification(psp, dp, n, errors.New("Missing topic"))
			p.pfp.OnPushFail(p, id, err)
		case 4:
			err := NewInvalidNotification(psp, dp, n, errors.New("Missing payload"))
			p.pfp.OnPushFail(p, id, err)
		case 5:
			err := NewInvalidNotification(psp, dp, n, errors.New("Invalid token size"))
			p.pfp.OnPushFail(p, id, err)
		case 6:
			err := NewInvalidNotification(psp, dp, n, errors.New("Invalid topic size"))
			p.pfp.OnPushFail(p, id, err)
		case 7:
			err := NewInvalidNotification(psp, dp, n, errors.New("Invalid payload size"))
			p.pfp.OnPushFail(p, id, err)
		case 8:
			err := NewInvalidDeliveryPointError(psp, dp, errors.New("Invalid token"))
			p.pfp.OnPushFail(p, id, err)
		default:
			err := errors.New("Unknown Error")
			p.pfp.OnPushFail(p, id, err)
		}
	}
}


func (self *apnsPushService) resultCollector(psp *PushServiceProvider, resChan chan<- *apnsResult, quit <-chan bool) {
	c, err := getConn(psp)
	if err != nil {
		res := new(apnsResult)
		res.err = NewConnectionError(err)
		resChan<-res
		return
	}

	for {
		readb := [6]byte{}
		nr, err := c.Read(readb[:])
		if err != nil {
			res := new(apnsResult)
			res.err = NewConnectionError(err)
			resChan<-res
			continue
		}
		if nr != 6 {
			res := new(apnsResult)
			res.err = NewConnectionError(fmt.Errorf("[APNS] Received %v bytes", nr))
			resChan<-res
			continue
		}

		buf := bytes.NewBuffer(readb)
		var cmd uint8
		var status uint8
		var msgid uint32

		err = binary.Read(buf, binary.BigEndian, &cmd)
		if err != nil {
			res := new(apnsResult)
			res.err = NewConnectionError(err)
			resChan<-res
			continue
		}

		err = binary.Read(buf, binary.BigEndian, &status)
		if err != nil {
			res := new(apnsResult)
			res.err = NewConnectionError(err)
			resChan<-res
			continue
		}

		err = binary.Read(buf, binary.BigEndian, &msgid)
		if err != nil {
			res := new(apnsResult)
			res.err = NewConnectionError(err)
			resChan<-res
			continue
		}


		res := new(apnsResult)
		res.msgId = msgid

		switch status {
		case 0:
			res.err = nil
		case 2:
			fallthrough
		case 8:
			res.err = NewBadDeliveryPoint(psp)
		case 3:
			fallthrough
		case 4:
			fallthrough
		case 5:
			fallthrough
		case 6:
			fallthrough
		case 7:
			res.err = NewBadNotification()
		default:
			res.err = fmt.Errorf("Unknown Error: %d", status)
		}
		resChan<-res
	}
}

func (self *apnsPushService) pushWorker(psp *PushServiceProvider, reqChan chan *pushRequest) {
	resChan := make(chan *apnsResult)

	reqIdMap := make(map[uint32]*pushRequest)

	var connErr error

	connErr = nil

	go resultCollector(psp, resChan)
	for {
		select {
		case req := <-reqChan:
			dp := req.dp
			notif := req.notif
			mid := req.mid

			if connErr != nil {
				result := new(PushResult)
				result.Content = notif
				result.Provider = psp
				result.Destination = dp
				result.MsgId = fmt.Sprintf("%v", mid)
				result.Err = connErr
				req.resChan<-result
				continue
			}

			reqIdMap[mid] = req
			err := self.singlePush(psp, dp, notif, mid)

			if err != nil {
				result := new(PushResult)
				result.Content = notif
				result.Provider = psp
				result.Destination = dp
				result.MsgId = fmt.Sprintf("%v", mid)
				result.Err = err
				req.resChan<-result
				delete(reqIdMap[mid])
			}

		case apnsres := <-resChan:
			if cerr, ok := apnsres.err.(*ConnectionError); ok {
				connErr = cerr
			}
			if req, ok := reqIdMap[apnsres.msgId]; ok {
				result := new(PushResult)
				result.Content = req.notif
				result.Provider = psp
				result.Destination = req.dp
				result.MsgId = fmt.Sprintf("%v", apnsres.msgId)
				result.Err = apnsres.err
				req.resChan<-result
				delete(reqIdMap[mid])
			}
		}
	}
}

func (p *apnsPushService) singlePush(sp *PushServiceProvider,
	s *DeliveryPoint,
	n *Notification) (string, error) {
	devtoken := s.FixedData["devtoken"]
	btoken, err := hex.DecodeString(devtoken)
	if err != nil {
		return "", NewInvalidDeliveryPointError(sp, s, err)
	}

	bpayload, err := toAPNSPayload(n)
	if err != nil {
		return "", NewInvalidNotification(sp, s, n, err)
	}
	buffer := bytes.NewBuffer([]byte{})
	// command
	binary.Write(buffer, binary.BigEndian, uint8(1))

	// transaction id
	mid := atomic.AddUint32(&(p.nextid), 1)
	if smid, ok := n.Data["id"]; ok {
		imid, err := strconv.ParseUint(smid, 10, 0)
		if err == nil {
			mid = uint32(imid)
		}
	}
	binary.Write(buffer, binary.BigEndian, mid)

	// Expiry
	expiry := uint32(time.Now().Second() + 60*60)

	if sexpiry, ok := n.Data["expiry"]; ok {
		uiexp, err := strconv.ParseUint(sexpiry, 10, 0)
		if err == nil {
			expiry = uint32(uiexp)
		}
	}
	binary.Write(buffer, binary.BigEndian, expiry)

	// device token
	binary.Write(buffer, binary.BigEndian, uint16(len(btoken)))
	binary.Write(buffer, binary.BigEndian, btoken)

	// payload
	binary.Write(buffer, binary.BigEndian, uint16(len(bpayload)))
	binary.Write(buffer, binary.BigEndian, bpayload)
	pdu := buffer.Bytes()

	tlsconn, err := p.getConn(sp)
	if err != nil {
		return "", err
	}

	for i := 0; i < 2; i++ {
		err = writen(tlsconn, pdu)
		if err != nil {
			tlsconn, err = p.reconnect(sp)
			if err != nil {
				return "", err
			}
		} else {
			break
		}
	}

	if err != nil {
		return "", err
	}

	id := fmt.Sprintf("apns:%s-%d", sp.Name(), mid)
	go p.waitError(id, tlsconn, sp, s, n)
	return id, nil
}
