package pushsrv

import (
"encoding/json"
)

func ApplyAlertLocMsg(alert map[string]interface{}, v string) (error) {
	var data map[string]interface{}

	var err = json.Unmarshal([]byte(v), &data)
	if err != nil {
		return err
	}

	for k, v := range data {
		switch k {
		case "loc-key":
			alert[k] = v
		case "loc-args":
			alert[k] = v
		}
	}

	return nil
}
