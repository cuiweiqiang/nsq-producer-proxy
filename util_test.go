/*
* @Author: cuiweiqiang
* @Date:   2018-09-10 12:37:20
* @Last Modified by:   cuiweiqiang
* @Last Modified time: 2018-09-26 11:22:07
 */
package producerProxy

import (
	"testing"
)

func Test_Get(t *testing.T) {
	uri := "https://www.baidu.com"
	result, err := Get(uri, nil)

	if err != nil {
		t.Error(string(result), err)
		t.FailNow()
	}
}

func Benchmark_Get(b *testing.B) {
	uri := "https://www.baidu.com"
	for i := 0; i < b.N; i++ {
		_, _ = Get(uri, nil)
	}
}
