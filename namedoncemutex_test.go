// Copyright 2021 Airbus Defence and Space
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package osio

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNamedOnceMutex(t *testing.T) {
	key := "foo"
	n1 := newNamedOnceMutex()
	l1 := n1.Lock(key)
	assert.True(t, l1)
	l1 = n1.TryLock(key)
	assert.False(t, l1)

	ww := make(chan bool)
	go func() {
		l1 = n1.Lock(key)
		assert.False(t, l1)
		close(ww)
	}()

	time.Sleep(10 * time.Millisecond)
	n1.Unlock(key)
	<-ww

	l1 = n1.Lock(key)
	assert.True(t, l1)
	n1.Unlock(key)

	l1 = n1.TryLock(key)
	assert.True(t, l1)
	l1 = n1.TryLock(key)
	assert.False(t, l1)

	ww = make(chan bool)
	go func() {
		l1 = n1.Lock(key)
		assert.False(t, l1)
		close(ww)
	}()
	time.Sleep(10 * time.Millisecond)
	n1.Unlock(key)
	<-ww
}
