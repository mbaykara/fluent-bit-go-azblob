package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

const (
	BlockSize        = 4 * 1024 * 1024 // 4m
	Parallelism      = 4
	Timeout          = 30
	PublicAccessType = azblob.PublicAccessNone
	MinCheckInterval = 50 * time.Millisecond
)

type Batch struct {
	Buffer    []byte
	CreatedAt time.Time
}

type Entry struct {
	TimeSlice string
	Raw       []byte
}

type Func func() error

type AzblobUploader struct {
	Entries    chan Entry
	batches    map[string]*Batch
	container  azblob.ContainerURL
	timeTicker *time.Ticker
	quit       chan struct{}
	once       sync.Once
	wg         sync.WaitGroup
	config     *AzblobConfig
	logger     *logrus.Entry
}

type Memorize struct {
	LogData string
}
type LogData struct {
	Stream     string `json:"stream"`
	Logtag     string `json:"logtag"`
	Message    string `json:"message"`
	Kubernetes Kubernetes
}

type Kubernetes struct {
	Pod       string `json:"pod_name"`
	Namespace string `json:"namespace_name"`
	Container string `json:"container_name"`
	Host      string `json:"host"`
	Image     string `json:"container_image"`
	Labels    Labels
}

type Labels struct {
	App      string `json:"app"`
	K8s_App  string `json:"k8s_app"`
	Type     string `json:"type"`
	Instance string `json:"app.kubernetes.io/instance"`
}

func NewUploader(c *AzblobConfig, l *logrus.Entry) (*AzblobUploader, error) {
	checkInterval := c.BatchWait / 10
	if checkInterval < MinCheckInterval {
		checkInterval = MinCheckInterval
	}

	u := &AzblobUploader{
		Entries:    make(chan Entry),
		batches:    map[string]*Batch{},
		container:  c.ContainerURL,
		timeTicker: time.NewTicker(checkInterval),
		quit:       make(chan struct{}),
		config:     c,
		logger:     l,
	}

	u.wg.Add(1)
	go u.start()

	return u, nil
}

func (u *AzblobUploader) start() {
	defer func() {
		for ts, b := range u.batches {
			u.sendBatch(ts, b.Buffer)
		}

		u.wg.Done()
	}()

	for {
		select {
		case <-u.quit:
			return
		case <-u.timeTicker.C:
			for ts, b := range u.batches {
				if time.Since(b.CreatedAt) < u.config.BatchWait {
					continue
				}

				u.logger.Debug("max wait time reached, sending batch...")
				go u.sendBatch(ts, b.Buffer)
				delete(u.batches, ts)
			}
		case e := <-u.Entries:
			batch, ok := u.batches[e.TimeSlice]

			if !ok {
				u.batches[e.TimeSlice] = &Batch{
					Buffer:    e.Raw,
					CreatedAt: time.Now(),
				}
				break
			}

			if uint64(len(batch.Buffer)) > u.config.BatchLimitSize {
				u.logger.Debug("max size reached, sending batch...")
				go u.sendBatch(e.TimeSlice, batch.Buffer)

				u.batches[e.TimeSlice] = &Batch{
					Buffer:    e.Raw,
					CreatedAt: time.Now(),
				}
				break
			}

			batch.Buffer = append(batch.Buffer, "\n"...)
			batch.Buffer = append(batch.Buffer, e.Raw...)
		}
	}
}

func (u *AzblobUploader) Stop() {
	u.once.Do(func() { close(u.quit) })
	u.wg.Wait()
}

func (u *AzblobUploader) sendBatch(timeSlice string, b []byte) {
	// Generate ObjectKey
	objectKey := u.config.ObjectKeyFormat
	objectKey = strings.ReplaceAll(objectKey, "%{hostname}", Hostname)
	objectKey = strings.ReplaceAll(objectKey, "%{uuid}", uuid.NewV4().String())
	objectKey = strings.ReplaceAll(objectKey, "%{time_slice}", timeSlice)

	u.logger.Debugf("upload blob=%s size: %d bytes", objectKey, len(b))
	a := string(b)

	bufio.NewScanner(strings.NewReader(a))
	for i, line := in bufio.ScanLines(b, true)

	logger.Info("---------- data start SendBactch")
	logger.Info("---------- data end SendBactch")

	var buf []byte
	var err error
	err = retry(u.config.BatchRetryLimit, func() error {
		switch u.config.StoreAs {
		case GzipFormat:
			buf, err = makeGzip(b)
			if err != nil {
				u.logger.Error(err.Error())
				return err
			}
		}

		return u.upload(objectKey, buf)
	})

	if err != nil {
		u.logger.Errorf("retry limit reached, blob=%s", objectKey)
	}
}

func retry(attempts *uint64, f Func) error {
	counter := uint64(0)
	interval := time.Second

	for {
		err := f()

		if err == nil {
			return nil
		}

		if attempts == nil || counter < *attempts {
			counter++

			// Add some randomness to prevent creating a Thundering Herd
			jitter := time.Duration(rand.Int63n(int64(interval)))
			interval = interval + jitter/2
			time.Sleep(interval)
			continue
		}

		return err
	}
}

// based on https://text.baldanders.info/golang/gzip-operation/
func makeGzip(buf []byte) ([]byte, error) {
	var b bytes.Buffer

	err := func() error {
		gw := gzip.NewWriter(&b)
		gw.Name = "fluent-bit-go-azblob"
		gw.ModTime = time.Now()

		defer gw.Close()

		if _, err := gw.Write(buf); err != nil {
			return err
		}
		return nil
	}()

	return b.Bytes(), err
}

func (u *AzblobUploader) upload(objectKey string, b []byte) error {
	ctx, cancel := context.WithTimeout(
		context.Background(), Timeout*time.Second)
	defer cancel()

	if u.config.AutoCreateContainer {
		err := u.ensureContainer(ctx)
		if err != nil {
			return err
		}
	}

	blobURL := u.container.NewBlockBlobURL(objectKey)
	options := azblob.UploadToBlockBlobOptions{
		BlockSize:   BlockSize,
		Parallelism: Parallelism,
	}

	_, err := azblob.UploadBufferToBlockBlob(ctx, b, blobURL, options)
	logger.Info("=========upload start=============")
	logger.Info(b)
	logger.Info("=========upload  end=============")
	if err != nil {
		u.logger.Errorf("upload to blob error: %s", err.Error())
		return err
	}

	return nil
}

func (u *AzblobUploader) ensureContainer(ctx context.Context) error {
	var err error

	_, err = u.container.GetProperties(ctx, azblob.LeaseAccessConditions{})
	if err == nil {
		return nil
	}

	_, err = u.container.Create(ctx, azblob.Metadata{}, PublicAccessType)
	if err != nil {
		return err
	}

	return nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
