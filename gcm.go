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
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/uniqush/pushsys"
	"io/ioutil"
	"net/http"
)

const (
	gcmServiceURL string = "https://android.googleapis.com/gcm/send"
)

type gcmPushService struct {
}

func newGCMPushService() *gcmPushService {
	ret := new(gcmPushService)
	return ret
}

func InstallGCM() {
	psm := GetPushServiceManager()
	psm.RegisterPushServiceType(newGCMPushService())
}

func (p *gcmPushService) SetAsyncFailureHandler(pf PushFailureHandler) {
}

func (p *gcmPushService) Finalize() {}

func (p *gcmPushService) BuildPushServiceProviderFromMap(kv map[string]string,
	psp *PushServiceProvider) error {
	if service, ok := kv["service"]; ok && len(service) > 0 {
		psp.FixedData["service"] = service
	} else {
		return errors.New("NoService")
	}

	if projectid, ok := kv["projectid"]; ok && len(projectid) > 0 {
		psp.FixedData["projectid"] = projectid
	} else {
		return errors.New("NoProjectID")
	}

	if authtoken, ok := kv["apikey"]; ok && len(authtoken) > 0 {
		psp.VolatileData["apikey"] = authtoken
	} else {
		return errors.New("NoAPIKey")
	}

	return nil
}

func (p *gcmPushService) BuildDeliveryPointFromMap(kv map[string]string,
	dp *DeliveryPoint) error {
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
	if account, ok := kv["account"]; ok && len(account) > 0 {
		dp.FixedData["account"] = account
	} else {
		return errors.New("NoGoogleAccount")
	}

	if regid, ok := kv["regid"]; ok && len(regid) > 0 {
		dp.FixedData["regid"] = regid
	} else {
		return errors.New("NoRegId")
	}

	return nil
}

func (p *gcmPushService) Name() string {
	return "gcm"
}

type gcmData struct {
	RegIDs         []string          `json:"registration_ids"`
	CollapseKey    string            `json:"collapse_key,omitempty"`
	Data           map[string]string `json:"data"`
	DelayWhileIdle bool              `json:"delay_while_idle,omitempty"`
	TimeToLive     uint              `json:"time_to_live,omitempty"`
}

func (d *gcmData) String() string {
	ret, err := json.Marshal(d)
	if err != nil {
		return ""
	}
	return string(ret)
}

type gcmResult struct {
	MulticastID  uint64              `json:"multicast_id"`
	Success      uint                `json:"success"`
	Failure      uint                `json:"failure"`
	CanonicalIDs uint                `json:"canonical_ids"`
	Results      []map[string]string `json:"results"`
}

func (p *gcmPushService) Push(psp *PushServiceProvider, dp *DeliveryPoint, n *Notification) (string, error) {
	if psp.PushServiceName() != dp.PushServiceName() ||
		psp.PushServiceName() != p.Name() {
		return "", NewPushIncompatibleError(psp, dp, p)
	}

	msg := n.Data
	data := new(gcmData)
	data.RegIDs = make([]string, 1)

	// TODO do something with ttl and delay_while_idle
	data.TimeToLive = 0
	data.DelayWhileIdle = false

	if regID, ok := dp.VolatileData["regid"]; ok {
		data.RegIDs[0] = regID
	} else {
		data.RegIDs[0] = dp.FixedData["regid"]
	}
	if len(data.RegIDs[0]) == 0 {
		reterr := NewInvalidDeliveryPointError(psp, dp, errors.New("EmptyRegistrationID"))
		return "", reterr
	}
	if mgroup, ok := msg["msggroup"]; ok {
		data.CollapseKey = mgroup
	} else {
		data.CollapseKey = ""
	}

	nr_elem := len(msg)
	data.Data = make(map[string]string, nr_elem)

	for k, v := range msg {
		switch k {
		case "msggroup":
			continue
		default:
			data.Data[k] = v
		}
	}

	jdata, err := json.Marshal(data)

	if err != nil {
		return "", errors.New("Json encoding error: " + err.Error())
	}

	req, err := http.NewRequest("POST", gcmServiceURL, bytes.NewReader(jdata))
	if err != nil {
		return "", err
	}

	apikey := psp.VolatileData["apikey"]

	req.Header.Set("Authorization", "key="+apikey)
	req.Header.Set("Content-Type", "application/json")

	conf := &tls.Config{InsecureSkipVerify: false}
	tr := &http.Transport{TLSClientConfig: conf}
	client := &http.Client{Transport: tr}

	fmt.Printf("Sending data %v\n", data)

	r, e20 := client.Do(req)
	if e20 != nil {
		return "", e20
	}
	refreshpsp := false
	new_auth_token := r.Header.Get("Update-Client-Auth")
	if new_auth_token != "" && apikey != new_auth_token {
		psp.VolatileData["apikey"] = new_auth_token
		refreshpsp = true
	}

	// TODO More GCM specific error handle
	switch r.StatusCode {
	case 503:
		/* TODO extract the retry after field */
		after := -1
		var reterr error
		reterr = NewRetryError(after)
		if refreshpsp {
			re := NewRefreshDataError(psp, nil, reterr)
			reterr = re
		}
		return "", reterr
	case 401:
		return "", NewInvalidPushServiceProviderError(psp, errors.New("Invalid Auth Token"))
	}

	contents, e30 := ioutil.ReadAll(r.Body)
	if e30 != nil {
		if refreshpsp {
			re := NewRefreshDataError(psp, nil, e30)
			e30 = re
		}
		return "", e30
	}

	var result gcmResult
	err = json.Unmarshal(contents, &result)

	if err != nil {
		return "", err
	}

    if errmsg, ok := result.Results[0]["error"]; ok {
        if errmsg == "Unavailable" {
            after := -1
            reterr := NewRetryError(after)
            return "", reterr
        }
		return "", errors.New("GCMErr" + errmsg)
    }

	if result.Failure > 0 {
		return "", errors.New(string(contents))
	}

    if newregid, ok := result.Results[0]["registration_id"]; ok {
        newdp := dp.Copy()
		newdp.VolatileData["regid"] = newregid
        reterr := NewRefreshDataError(nil, newdp, nil)
        return "", reterr
    }

	return result.Results[0]["message_id"], nil
}
