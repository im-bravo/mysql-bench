package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type RateLimiter interface {
	Wait()
	ExpectedInterval() int64
}

type UnlimitedRateLimiter struct{}

func (*UnlimitedRateLimiter) Wait() {}

func (*UnlimitedRateLimiter) ExpectedInterval() int64 {
	return 0
}

type MaximumRateLimiter struct {
	Period              time.Duration
	StartTime           time.Time
	CompletedOperations int64
}

func (mxrl *MaximumRateLimiter) Wait() {
	mxrl.CompletedOperations++
	nextRequest := mxrl.StartTime.Add(mxrl.Period * time.Duration(mxrl.CompletedOperations))
	now := time.Now()
	if now.Before(nextRequest) {
		time.Sleep(nextRequest.Sub(now))
	}
}

func (mxrl *MaximumRateLimiter) ExpectedInterval() int64 {
	return mxrl.Period.Nanoseconds()
}

func NewRateLimiter(maximumRate int, timeOffset time.Duration) RateLimiter {
	if maximumRate == 0 {
		return &UnlimitedRateLimiter{}
	}
	period := time.Duration(int64(time.Second) / int64(maximumRate))
	return &MaximumRateLimiter{period, time.Now(), 0}
}

type Result struct {
	Final          bool
	ElapsedTime    time.Duration
	Operations     int
	ClusteringRows int
	Errors         int
	Latency        *hdrhistogram.Histogram
}

type MergedResult struct {
	Time                    time.Duration
	Operations              int
	ClusteringRows          int
	OperationsPerSecond     float64
	ClusteringRowsPerSecond float64
	Errors                  int
	Latency                 *hdrhistogram.Histogram
}

func NewMergedResult() *MergedResult {
	result := &MergedResult{}
	result.Latency = NewHistogram()
	return result
}

func (mr *MergedResult) AddResult(result Result) {
	mr.Time += result.ElapsedTime
	mr.Operations += result.Operations
	mr.ClusteringRows += result.ClusteringRows
	mr.OperationsPerSecond += float64(result.Operations) / result.ElapsedTime.Seconds()
	mr.ClusteringRowsPerSecond += float64(result.ClusteringRows) / result.ElapsedTime.Seconds()
	mr.Errors += result.Errors
	if measureLatency {
		dropped := mr.Latency.Merge(result.Latency)
		if dropped > 0 {
			log.Print("dropped: ", dropped)
		}
	}
}

func NewHistogram() *hdrhistogram.Histogram {
	if !measureLatency {
		return nil
	}
	return hdrhistogram.New(time.Microsecond.Nanoseconds()*50, (timeout + timeout*2).Nanoseconds(), 3)
}

func HandleError(err error) {
	if atomic.SwapUint32(&stopAll, 1) == 0 {
		log.Print(err)
		fmt.Println("\nstopping")
		atomic.StoreUint32(&stopAll, 1)
	}
}

func MergeResults(results []chan Result) (bool, *MergedResult) {
	result := NewMergedResult()
	final := false
	for i, ch := range results {
		res := <-ch
		if !final && res.Final {
			final = true
			result = NewMergedResult()
			for _, ch2 := range results[0:i] {
				res = <-ch2
				for !res.Final {
					res = <-ch2
				}
				result.AddResult(res)
			}
		} else if final && !res.Final {
			for !res.Final {
				res = <-ch
			}
		}
		result.AddResult(res)
	}
	result.Time /= time.Duration(concurrency)
	return final, result
}

func RunConcurrently(maximumRate int, workload func(id int, resultChannel chan Result, rateLimiter RateLimiter)) *MergedResult {
	var timeOffsetUnit int64
	if maximumRate != 0 {
		timeOffsetUnit = int64(time.Second) / int64(maximumRate)
		maximumRate /= concurrency
	} else {
		timeOffsetUnit = 0
	}

	results := make([]chan Result, concurrency)
	for i := range results {
		results[i] = make(chan Result, 1)
	}

	startTime := time.Now()
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			timeOffset := time.Duration(timeOffsetUnit * int64(i))
			workload(i, results[i], NewRateLimiter(maximumRate, timeOffset))
			close(results[i])
		}(i)
	}

	final, result := MergeResults(results)
	for !final {
		result.Time = time.Now().Sub(startTime)
		PrintPartialResult(result)
		final, result = MergeResults(results)
	}
	return result
}

type ResultBuilder struct {
	FullResult    *Result
	PartialResult *Result
}

func NewResultBuilder() *ResultBuilder {
	rb := &ResultBuilder{}
	rb.FullResult = &Result{}
	rb.PartialResult = &Result{}
	rb.FullResult.Final = true
	rb.FullResult.Latency = NewHistogram()
	rb.PartialResult.Latency = NewHistogram()
	return rb
}

func (rb *ResultBuilder) IncOps() {
	rb.FullResult.Operations++
	rb.PartialResult.Operations++
}

func (rb *ResultBuilder) IncRows() {
	rb.FullResult.ClusteringRows++
	rb.PartialResult.ClusteringRows++
}

func (rb *ResultBuilder) AddRows(n int) {
	rb.FullResult.ClusteringRows += n
	rb.PartialResult.ClusteringRows += n
}

func (rb *ResultBuilder) IncErrors() {
	rb.FullResult.Errors++
	rb.PartialResult.Errors++
}

func (rb *ResultBuilder) ResetPartialResult() {
	rb.PartialResult = &Result{}
	rb.PartialResult.Latency = NewHistogram()
}

func (rb *ResultBuilder) RecordLatency(latency time.Duration, rateLimiter RateLimiter) error {
	if !measureLatency {
		return nil
	}

	err := rb.FullResult.Latency.RecordCorrectedValue(latency.Nanoseconds(), rateLimiter.ExpectedInterval())
	if err != nil {
		return err
	}

	err = rb.PartialResult.Latency.RecordCorrectedValue(latency.Nanoseconds(), rateLimiter.ExpectedInterval())
	if err != nil {
		return err
	}

	return nil
}

var errorRecordingLatency bool

type TestIterator struct {
	iteration uint
	workload  WorkloadGenerator
}

func NewTestIterator(workload WorkloadGenerator) *TestIterator {
	return &TestIterator{0, workload}
}

func (ti *TestIterator) IsDone() bool {
	if atomic.LoadUint32(&stopAll) != 0 {
		return true
	}

	if ti.workload.IsDone() {
		if ti.iteration+1 == iterations {
			return true
		} else {
			ti.workload.Restart()
			ti.iteration++
			return false
		}
	} else {
		return false
	}
}

func RunTest(resultChannel chan Result, workload WorkloadGenerator, rateLimiter RateLimiter, test func(rb *ResultBuilder) (error, time.Duration)) {
	rb := NewResultBuilder()

	start := time.Now()
	partialStart := start
	iter := NewTestIterator(workload)
	for !iter.IsDone() {
		rateLimiter.Wait()

		err, latency := test(rb)
		if err != nil {
			log.Print(err)
			rb.IncErrors()
			continue
		}

		err = rb.RecordLatency(latency, rateLimiter)
		if err != nil {
			errorRecordingLatency = true
		}

		now := time.Now()
		if now.Sub(partialStart) > time.Second {
			resultChannel <- *rb.PartialResult
			rb.ResetPartialResult()
			partialStart = now
		}
	}
	end := time.Now()

	rb.FullResult.ElapsedTime = end.Sub(start)
	resultChannel <- *rb.FullResult
}

const (
	generatedDataHeaderSize int64 = 24
	generatedDataMinSize    int64 = generatedDataHeaderSize + 33
)

func GenerateData(pk int64, ck int64, size int64) []byte {
	if !validateData {
		return make([]byte, size)
	}

	buf := new(bytes.Buffer)

	if size < generatedDataHeaderSize {
		binary.Write(buf, binary.LittleEndian, int8(size))
		binary.Write(buf, binary.LittleEndian, pk^ck)
	} else {
		binary.Write(buf, binary.LittleEndian, size)
		binary.Write(buf, binary.LittleEndian, pk)
		binary.Write(buf, binary.LittleEndian, ck)
		if size < generatedDataMinSize {
			for i := generatedDataHeaderSize; i < size; i++ {
				binary.Write(buf, binary.LittleEndian, int8(0))
			}
		} else {
			payload := make([]byte, size-generatedDataHeaderSize-sha256.Size)
			rand.Read(payload)
			csum := sha256.Sum256(payload)
			binary.Write(buf, binary.LittleEndian, payload)
			binary.Write(buf, binary.LittleEndian, csum)
		}
	}

	value := make([]byte, size)
	copy(value, buf.Bytes())
	return value
}

func ValidateData(pk int64, ck int64, data []byte) error {
	if !validateData {
		return nil
	}

	buf := bytes.NewBuffer(data)
	size := int64(buf.Len())

	var storedSize int64
	if size < generatedDataHeaderSize {
		var storedSizeCompact int8
		err := binary.Read(buf, binary.LittleEndian, &storedSizeCompact)
		if err != nil {
			return errors.Wrap(err, "failed to validate data, cannot read size from value")
		}
		storedSize = int64(storedSizeCompact)
	} else {
		err := binary.Read(buf, binary.LittleEndian, &storedSize)
		if err != nil {
			return errors.Wrap(err, "failed to validate data, cannot read size from value")
		}
	}

	if size != storedSize {
		return errors.Errorf("actual size of value (%d) doesn't match size stored in value (%d)", size, storedSize)
	}

	// There is no random payload for sizes < minFullSize
	if size < generatedDataMinSize {
		expectedBuf := GenerateData(pk, ck, size)
		if !bytes.Equal(buf.Bytes(), expectedBuf) {
			return errors.Errorf("actual value doesn't match expected value:\nexpected: %x\nactual: %x", expectedBuf, buf.Bytes())
		}
		return nil
	}

	var storedPk, storedCk int64
	var err error

	// Validate pk
	err = binary.Read(buf, binary.LittleEndian, &storedPk)
	if err != nil {
		return errors.Wrap(err, "failed to validate data, cannot read pk from value")
	}
	if storedPk != pk {
		return errors.Errorf("actual pk (%d) doesn't match pk stored in value (%d)", pk, storedPk)
	}

	// Validate ck
	err = binary.Read(buf, binary.LittleEndian, &storedCk)
	if err != nil {
		return errors.Wrap(err, "failed to validate data, cannot read pk from value")
	}
	if storedCk != ck {
		return errors.Errorf("actual ck (%d) doesn't match ck stored in value (%d)", ck, storedCk)
	}

	// Validate checksum over the payload
	payload := make([]byte, size-generatedDataHeaderSize-sha256.Size)
	err = binary.Read(buf, binary.LittleEndian, payload)
	if err != nil {
		return errors.Wrap(err, "failed to verify checksum, cannot read payload from value")
	}

	calculatedChecksumArray := sha256.Sum256(payload)
	calculatedChecksum := calculatedChecksumArray[0:]

	storedChecksum := make([]byte, 32)
	err = binary.Read(buf, binary.LittleEndian, storedChecksum)
	if err != nil {
		return errors.Wrap(err, "failed to verify checksum, cannot read checksum from value")
	}

	if !bytes.Equal(calculatedChecksum, storedChecksum) {
		return errors.New(fmt.Sprintf(
			"corrupt checksum or data: calculated checksum (%x) doesn't match stored checksum (%x) over data\n%x",
			calculatedChecksum,
			storedChecksum,
			payload))
	}
	return nil
}

func DoWrites(session *gocql.Session, resultChannel chan Result, workload WorkloadGenerator, rateLimiter RateLimiter) {
	query := session.Query("INSERT INTO " + keyspaceName + "." + tableName + " (pk, ck, v) VALUES (?, ?, ?)")

	RunTest(resultChannel, workload, rateLimiter, func(rb *ResultBuilder) (error, time.Duration) {
		pk := workload.NextPartitionKey()
		ck := workload.NextClusteringKey()
		value := GenerateData(pk, ck, clusteringRowSizeDist.Generate())
		bound := query.Bind(pk, ck, value)

		requestStart := time.Now()
		err := bound.Exec()
		requestEnd := time.Now()
		if err != nil {
			return err, time.Duration(0)
		}

		rb.IncOps()
		rb.IncRows()

		latency := requestEnd.Sub(requestStart)
		return nil, latency
	})
}

func DoWritesMySQL(session *sql.DB, resultChannel chan Result, workload WorkloadGenerator, rateLimiter RateLimiter) {
	sqlinsert := "INSERT INTO grant_standard ( " +
		" tx_id " +
		", request_key " +
		", user_id " +
		", client_id " +
		", granted " +
		", request_sub_key " +
		", currency " +
		", shop_id " +
		", campaign_id " +
		", history_url " + // 10
		", history_text " +
		", item_name " +
		", item_url " +
		", item_price " +
		", order_no " +
		", order_url " +
		", order_total " +
		", fix_flag " +
		", fix_time " +
		", tx_time " +
		" ) VALUES (" +
		" ? " +
		", ? " +
		", ? " +
		", ? " +
		", ? " +
		", ? " +
		", ? " +
		", ? " +
		", ? " +
		", ? " + // 10
		", ? " +
		", ? " +
		", ? " + // item url
		", ? " +
		", ? " +
		", ? " +
		", ? " +
		", ? " + // fix_flag
		", ? " +
		", ? " +
		") "

	query, err := session.Prepare(sqlinsert)
	// query := session.Query("INSERT INTO " + keyspaceName + "." + tableName + " (tx_id,request_key) VALUES (?,?)")

	if err != nil {
		return
	}

	RunTest(resultChannel, workload, rateLimiter, func(rb *ResultBuilder) (error, time.Duration) {
		pk := workload.NextPartitionKey()
		ck := workload.NextClusteringKey()
		tx_id, nil := gocql.RandomUUID()
		request_key, nil := gocql.RandomUUID()
		user_id, nil := gocql.RandomUUID()
		campaign_id, nil := gocql.RandomUUID()
		client_id := ck
		granted := pk + 1000
		item_price := 12000
		order_no := "20930010920092043240"
		order_url := "https://basket.step.rakuten.co.jp/rms/mall/bs/cartempty/;jsessionid=RFB--5L1oedTVZLU1qjkwK386hwGCvAmH6HvYOMsSk8_YL2a0FuR!-582183230"
		order_total := 1000
		nowtime := time.Now()

		history_url := "https://item.rakuten.co.jp/oralb-braun/eb-pro500-kset_cp02/?s-id=top_normal_superdeal_pc"
		history_text := "ブラウン オーラルB 電動歯ブラシ pro500 & すみずみクリーンキッズ ファミリーセット | Braun Oral-B 公式ストア 正規品 セット 本体 ブラウン電動歯ブラシ ピカチュウ ポケモン 子ども 子供 子供用 キッズ ピカチュー cp2"
		item_name := "The Bull [ブル]"
		item_url := "https://item.rakuten.co.jp/offinet-kagu/10690332/?s-id=top_normal_target_ads&iasid=07rpp_31021_20028__e7-k53kmrbv-7jv-532d2cbd-3eb4-4077-bd35-933615c634a4"
		requestStart := time.Now()
		lres, err := query.Exec(
			tx_id.String(), request_key.String(), user_id.String(), client_id, granted, "1", "JPY", "1000990", campaign_id.String(), history_url, history_text, item_name, item_url, item_price, order_no, order_url, order_total, true, nowtime, nowtime)
		requestEnd := time.Now()
		if err != nil {
			return err, time.Duration(0)
		}

		affected, err := lres.RowsAffected()

		if affected != 1 {
			return err, time.Duration(0)
		}

		rb.IncOps()
		rb.IncRows()

		latency := requestEnd.Sub(requestStart)
		return nil, latency
	})
}
