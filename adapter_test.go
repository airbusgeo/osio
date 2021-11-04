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
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAdapterOptions(t *testing.T) {
	ccache, _ := NewLRUCache(0)
	kr := TReader{[]byte("foo")}
	_, err := NewAdapter(kr, BlockSize("-1"))
	assert.Error(t, err)
	_, err = NewAdapter(kr, BlockSize(""))
	assert.Error(t, err)
	_, err = NewAdapter(kr, BlockSize("foo"))
	assert.Error(t, err)
	_, err = NewAdapter(kr, BlockSize("1g"))
	assert.Error(t, err)
	_, err = NewAdapter(kr, BlockSize("1-1"))
	assert.Error(t, err)
	_, err = NewAdapter(kr, BlockSize("-1k"))
	assert.Error(t, err)
	_, err = NewAdapter(kr, BlockSize("-1kb"))
	assert.Error(t, err)
	_, err = NewAdapter(kr, BlockSize("1kb"))
	assert.NoError(t, err)
	_, err = NewAdapter(kr, BlockSize("1Kb"))
	assert.NoError(t, err)
	_, err = NewAdapter(kr, BlockSize("1b"))
	assert.NoError(t, err)
	_, err = NewAdapter(kr, BlockSize("1B"))
	assert.NoError(t, err)
	_, err = NewAdapter(kr, BlockSize("1mb"))
	assert.NoError(t, err)
	_, err = NewAdapter(kr, BlockSize("1mB"))
	assert.NoError(t, err)
	_, err = NewAdapter(kr, NumCachedBlocks(0))
	assert.Error(t, err)
	_, err = NewAdapter(kr, NumCachedBlocks(-1))
	assert.Error(t, err)
	_, err = NewAdapter(kr, NumCachedBlocks(10), BlockCache(ccache))
	assert.Error(t, err)
	_, err = NewAdapter(kr, NumCachedBlocks(10), BlockCache(nil))
	assert.Error(t, err)
	_, err = NewAdapter(kr, SizeCache(-10))
	assert.Error(t, err)
	_, err = NewAdapter(kr, SizeCache(100))
	assert.NoError(t, err)
}

type TReader struct {
	data []byte
}

var delay time.Duration
var errOver50 = errors.New("ff50")
var errRandom = errors.New("pseudo-random error")

func (r TReader) StreamAt(key string, off int64, n int64) (io.ReadCloser, int64, error) {
	ll := int64(len(r.data))
	if key == "fail_over_50" {
		if off >= 50 {
			time.Sleep(10 * time.Millisecond)
			return nil, 0, errOver50
		}
	} else {
		time.Sleep(delay)
	}
	if key == "enoent" {
		return nil, 0, syscall.ENOENT
	}
	if off < 0 {
		return nil, 0, errors.New("negative offset")
	}
	if off > 1024+40 {
		return nil, 0, errRandom
	}
	if off >= ll {
		return nil, ll, io.EOF
	}
	end := off + n
	if end > ll {
		return ioutil.NopCloser(bytes.NewReader(r.data[off:])), ll, io.EOF
	}
	return ioutil.NopCloser(bytes.NewReader(r.data[off:end])), ll, nil
}

type EReader struct {
	errbuf []byte
	delay  time.Duration
	erroff int64
	err    error
}

func (r EReader) StreamAt(key string, off int64, n int64) (io.ReadCloser, int64, error) {
	ll := int64(len(r.errbuf))
	time.Sleep(r.delay)
	if r.err != nil && off >= r.erroff {
		return nil, 0, r.err
	}
	if off >= ll {
		return nil, ll, io.EOF
	}
	end := off + n
	if end > ll {
		return ioutil.NopCloser(bytes.NewReader(r.errbuf[off:])), ll, io.EOF
	}
	return ioutil.NopCloser(bytes.NewReader(r.errbuf[off:end])), ll, nil
}

var rr TReader

func init() {
	data := make([]byte, 256*4)
	for i := byte(0); i <= 255; i++ {
		copy(data[int(i)*4:], []byte{i, i, i, i})
		if i == 255 {
			break
		}
	}
	rr = TReader{data}
}

var gsp bool
var gbs int
var gnb int

func test(t *testing.T, bc *Adapter, buf []byte, offset int64, expectedLen int, expected []byte, oneOfErr ...error) {
	t.Helper()
	//t.Logf("read [%d-%d]", offset, offset+int64(len(buf)))
	r, err := bc.ReadAt("", buf, offset)
	errok := false
	for _, eerr := range oneOfErr {
		if errors.Is(err, eerr) {
			errok = true
		}
	}
	if !errok {
		t.Errorf("got error %v, expected one of %v", err, oneOfErr)
	}
	if r != expectedLen {
		t.Errorf("got %d bytes, expected %d", r, expectedLen)
	}
	if !bytes.Equal(buf[0:r], expected) {
		t.Errorf("got %v, expected %v (%v,%v,%v)", buf[0:r], expected, gsp, gbs, gnb)
	}

}

func TestBlockCache(t *testing.T) {
	for blockSize := 1; blockSize < 20; blockSize++ {
		for cacheSize := 1; cacheSize < 20; cacheSize++ {
			//t.Logf("bs: %d, cs:%d", blockSize, cacheSize)
			testBlockCache(t, true, blockSize, cacheSize)
			testBlockCache(t, false, blockSize, cacheSize)
		}
	}
	cache, _ := NewLRUCache(10)
	bc, _ := NewAdapter(rr, BlockSize("10"), BlockCache(cache))
	for i := 1; i < 20; i++ {
		buf := make([]byte, i)
		for j := 0; j < 20; j++ {
			_, err := bc.ReadAt("enoent", buf, int64(j))
			if !errors.Is(err, syscall.ENOENT) {
				t.Error(err)
			}
		}
	}
}

func testBlockCache(t *testing.T, split bool, blockSize int, numCachedBlocks int) {
	gsp = split
	gbs = blockSize
	gnb = numCachedBlocks

	cache, _ := NewLRUCache(numCachedBlocks)
	bc, _ := NewAdapter(rr, BlockCache(cache), BlockSize(fmt.Sprintf("%d", blockSize)), SplitRanges(split))

	buf := make([]byte, 4)
	buf2 := make([]byte, 4)
	wg := sync.WaitGroup{}
	delay = 2 * time.Millisecond
	wg.Add(2)
	go func() {
		defer wg.Done()
		test(t, bc, buf, 0, 4, []byte{0, 0, 0, 0}, nil)
	}()
	go func() {
		defer wg.Done()
		test(t, bc, buf2, 0, 4, []byte{0, 0, 0, 0}, nil)
	}()
	wg.Wait()
	wg.Add(2)
	go func() {
		defer wg.Done()
		buf := make([]byte, 16)
		test(t, bc, buf, 63, 16, []byte{15, 16, 16, 16, 16, 17, 17, 17, 17, 18, 18, 18, 18, 19, 19, 19}, nil)
	}()
	go func() {
		defer wg.Done()
		buf := make([]byte, 16)
		test(t, bc, buf, 63, 16, []byte{15, 16, 16, 16, 16, 17, 17, 17, 17, 18, 18, 18, 18, 19, 19, 19}, nil)
	}()
	wg.Wait()
	delay = 0
	test(t, bc, buf, 2, 4, []byte{0, 0, 1, 1}, nil)
	test(t, bc, buf, 0, 4, []byte{0, 0, 0, 0}, nil)
	test(t, bc, buf, 2, 4, []byte{0, 0, 1, 1}, nil)
	buf = make([]byte, 8)
	test(t, bc, buf, 0, 8, []byte{0, 0, 0, 0, 1, 1, 1, 1}, nil)
	test(t, bc, buf, 2, 8, []byte{0, 0, 1, 1, 1, 1, 2, 2}, nil)
	test(t, bc, buf, 2, 8, []byte{0, 0, 1, 1, 1, 1, 2, 2}, nil)
	cache.Purge()
	test(t, bc, buf, 255*4, 4, []byte{255, 255, 255, 255}, io.EOF)
	test(t, bc, buf, 255*4-2, 6, []byte{254, 254, 255, 255, 255, 255}, io.EOF)
	test(t, bc, buf, 255*4-2, 6, []byte{254, 254, 255, 255, 255, 255}, io.EOF)
	test(t, bc, buf, 253*4, 8, []byte{253, 253, 253, 253, 254, 254, 254, 254}, nil)
	test(t, bc, buf, 255*4+2, 2, []byte{255, 255}, io.EOF)
	test(t, bc, buf, 256*4, 0, []byte{}, io.EOF)
	test(t, bc, buf, 256*4+2, 0, []byte{}, io.EOF) //outside bounds, but first block touches last data block
	test(t, bc, buf, 256*4+5, 0, []byte{}, io.EOF)
	buf = make([]byte, 12)
	test(t, bc, buf[0:4], 200*4, 4, []byte{200, 200, 200, 200}, nil)
	test(t, bc, buf, 200*4, 12, []byte{200, 200, 200, 200, 201, 201, 201, 201, 202, 202, 202, 202}, nil)
	test(t, bc, buf, 198*4, 12, []byte{198, 198, 198, 198, 199, 199, 199, 199, 200, 200, 200, 200}, nil)

	buf = make([]byte, 4)
	test(t, bc, buf, 0, 4, []byte{0, 0, 0, 0}, nil)
	test(t, bc, buf, 0, 4, []byte{0, 0, 0, 0}, nil)
	test(t, bc, buf, 2, 4, []byte{0, 0, 1, 1}, nil)
	test(t, bc, buf, 0, 4, []byte{0, 0, 0, 0}, nil)
	test(t, bc, buf, 2, 4, []byte{0, 0, 1, 1}, nil)
	buf = make([]byte, 8)
	test(t, bc, buf, 0, 8, []byte{0, 0, 0, 0, 1, 1, 1, 1}, nil)
	test(t, bc, buf, 2, 8, []byte{0, 0, 1, 1, 1, 1, 2, 2}, nil)
	test(t, bc, buf, 2, 8, []byte{0, 0, 1, 1, 1, 1, 2, 2}, nil)
	test(t, bc, buf, 255*4, 4, []byte{255, 255, 255, 255}, io.EOF)
	test(t, bc, buf, 255*4+2, 2, []byte{255, 255}, io.EOF)
	test(t, bc, buf, 256*4, 0, []byte{}, io.EOF)
	test(t, bc, buf, 256*4+2, 0, []byte{}, io.EOF) //outside bounds, but first block touches last data block
	test(t, bc, buf, 256*4+5, 0, []byte{}, io.EOF)
	cache.Purge()
	buf = make([]byte, 64)
	test(t, bc, buf, 255*4, 4, []byte{255, 255, 255, 255}, errRandom, io.EOF) //test non EOF error
	cache.Purge()

	//read before and after an already cached block
	buf = make([]byte, blockSize*4)
	exp, _, _ := rr.StreamAt("", int64(blockSize*3-blockSize/2), int64(blockSize*4))
	expx, _ := ioutil.ReadAll(exp)
	_, _ = bc.ReadAt("", buf[0:blockSize], int64(blockSize*3))
	test(t, bc, buf, int64(blockSize*3-blockSize/2), len(expx), expx, nil)

}

func TestMultiRead(t *testing.T) {
	delay = time.Millisecond
	cache, _ := NewLRUCache(100)
	bc, _ := NewAdapter(rr, BlockCache(cache), BlockSize("4"))
	bufs := [][]byte{
		{0, 0, 0, 0}, {0, 0, 0, 0}}
	offs := []int64{0, 4}
	_, err := bc.ReadAtMulti("", bufs, offs)
	assert.NoError(t, err)

	offs = []int64{0, 4}
	_, err = bc.ReadAtMulti("enoent", bufs, offs)
	assert.ErrorIs(t, err, syscall.ENOENT)

	offs = []int64{8, 1022}
	_, err = bc.ReadAtMulti("", bufs, offs)
	assert.ErrorIs(t, err, io.EOF)

	offs = []int64{8, 1025}
	_, err = bc.ReadAtMulti("", bufs, offs)
	assert.ErrorIs(t, err, io.EOF)

	offs = []int64{16, 52}
	_, err = bc.ReadAtMulti("fail_over_50", bufs, offs)
	assert.ErrorIs(t, err, errOver50)

	offs = []int64{58, 80}
	_, err = bc.ReadAtMulti("fail_over_50", bufs, offs)
	assert.ErrorIs(t, err, errOver50)
}

func TestReader(t *testing.T) {
	bc, _ := NewAdapter(rr)
	_, err := bc.Reader("enoent")
	assert.ErrorIs(t, err, syscall.ENOENT)
	_, err = bc.Reader("enoent") //from size cache
	assert.ErrorIs(t, err, syscall.ENOENT)
	bc, _ = NewAdapter(rr, BlockSize("2k"))
	r, err := bc.Reader("")
	assert.NoError(t, err)
	assert.Equal(t, int64(1024), r.Size())

	buf := make([]byte, 4)
	n, err := r.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte{0, 0, 0, 0}, buf)
	_, _ = r.Read(buf)
	assert.Equal(t, []byte{1, 1, 1, 1}, buf)

	_, _ = r.Seek(4, io.SeekCurrent)
	_, _ = r.Read(buf)
	assert.Equal(t, []byte{3, 3, 3, 3}, buf)

	_, _ = r.Seek(4, io.SeekStart)
	_, _ = r.Read(buf)
	assert.Equal(t, []byte{1, 1, 1, 1}, buf)

	_, err = r.Seek(-12, io.SeekCurrent)
	assert.ErrorIs(t, err, os.ErrInvalid)
	//check off hasn't changed
	_, _ = r.Read(buf)
	assert.Equal(t, []byte{2, 2, 2, 2}, buf)

	_, _ = r.Seek(-2, io.SeekEnd)
	_, err = r.Read(buf)
	assert.Equal(t, []byte{255, 255, 2, 2}, buf)
	assert.Equal(t, err, io.EOF)

	_, _ = r.Seek(4, io.SeekEnd)
	_, err = r.Read(buf)
	assert.ErrorIs(t, err, io.EOF)

	_, err = r.Seek(-4, -1)
	assert.ErrorIs(t, err, os.ErrInvalid)

	//check readat
	_, _ = r.Seek(0, io.SeekStart)
	_, _ = r.ReadAt(buf, 4)
	assert.Equal(t, []byte{1, 1, 1, 1}, buf)
	_, _ = r.Read(buf)
	assert.Equal(t, []byte{0, 0, 0, 0}, buf)

	n, err = r.ReadAt(buf, 1022)
	assert.Equal(t, 2, n)
	assert.Equal(t, io.EOF, err)

	n, err = r.ReadAt(buf, 1024)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	//check readatmulti
	bufs := [][]byte{
		{0, 0, 0, 0},
		{0, 0, 0, 0},
	}
	_, _ = r.ReadAtMulti(bufs, []int64{1, 2})
	assert.Equal(t, []byte{0, 0, 0, 1}, bufs[0])
	assert.Equal(t, []byte{0, 0, 1, 1}, bufs[1])

	_, _ = r.ReadAtMulti(bufs, []int64{1, 1020})
	assert.Equal(t, []byte{0, 0, 0, 1}, bufs[0])
	assert.Equal(t, []byte{255, 255, 255, 255}, bufs[1])

	_, err = r.ReadAtMulti(bufs, []int64{5, 1025})
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []byte{0, 0, 0, 1}, bufs[0])
	assert.Equal(t, []byte{255, 255, 255, 255}, bufs[1])

}

//provide test coverage of reader errors in multi-block requests
func TestRangeErrors(t *testing.T) {
	//check small last block
	er := EReader{
		delay:  100 * time.Millisecond,
		errbuf: []byte("abcd-efgh-ijkl-m"),
	}
	bc, _ := NewAdapter(er, BlockSize("5"))

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		buf := make([]byte, 15)
		_, err := bc.ReadAt("", buf, 0)
		assert.NoError(t, err)
	}()
	time.Sleep(5 * time.Millisecond)
	go func() {
		defer wg.Done()
		buf := make([]byte, 7)
		_, err := bc.ReadAt("", buf, 11)
		assert.ErrorIs(t, err, io.EOF)
	}()
	wg.Wait()

	//check reader error returned
	er = EReader{
		delay:  100 * time.Millisecond,
		errbuf: []byte("abcd-efgh-ijkl-m"),
		erroff: 15,
		err:    fmt.Errorf("foo"),
	}
	bc, _ = NewAdapter(er, BlockSize("5"))

	wg.Add(2)
	go func() {
		defer wg.Done()
		buf := make([]byte, 15)
		_, err := bc.ReadAt("", buf, 0)
		assert.NoError(t, err)
	}()
	time.Sleep(5 * time.Millisecond)
	go func() {
		defer wg.Done()
		buf := make([]byte, 15)
		_, err := bc.ReadAt("", buf, 6)
		assert.Equal(t, "foo", err.Error())
	}()
	wg.Wait()
}

type logger struct {
	last string
}

func (l *logger) Log(key string, off, len int64) {
	l.last = fmt.Sprintf("%s: %d-%d", key, off, len)
}
func TestLogging(t *testing.T) {
	ll := &logger{}
	bc, _ := NewAdapter(rr, WithLogger(ll))
	r, _ := bc.Reader("thekey")
	buf := make([]byte, 4)
	_, _ = r.Read(buf)
	assert.Equal(t, "thekey: 0-131072", ll.last)

	var lbuf bytes.Buffer
	log.SetOutput(&lbuf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()
	bc, _ = NewAdapter(rr, WithLogger(StdLogger))
	r, _ = bc.Reader("thekey")
	_, _ = r.Read(buf)
	assert.Contains(t, lbuf.String(), "GET thekey off=0 len=131072")
}
