

package pushsrv

import (
"encoding/json"
"fmt"
)

func toLocArgs(v string) (interface{}, error) {
	var data map[string]interface{};
	var err = json.Unmarshal([]byte(v), &data);
	if err != nil {
		fmt.Printf("err:%d\n", err);
	}
	return data["args"], err;
}

