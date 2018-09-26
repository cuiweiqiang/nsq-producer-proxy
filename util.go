/*
* @Author: cuiweiqiang
* @Date:   2018-09-10 12:37:26
* @Last Modified by:   cuiweiqiang
* @Last Modified time: 2018-09-26 11:22:04
 */
package producerProxy

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

func Get(uri string, args map[string]string) ([]byte, error) {
	u, _ := url.Parse(strings.Trim(uri, "/"))
	q := u.Query()
	if nil != args {
		for arg, val := range args {
			q.Add(arg, val)
		}
	}

	u.RawQuery = q.Encode()
	res, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http statusCode:%d", res.StatusCode)
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}

	return result, nil
}
