package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	az "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/kelseyhightower/envconfig"
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
	scanner := bufio.NewScanner(strings.NewReader(a))
	scanner.Split(bufio.ScanLines)
	var data []string
	for scanner.Scan() {
		data = append(data, scanner.Text())
	}
	store := "["
	for i := 0; i < len(data); i++ {
		store = store + data[i] + ","
	}
	store = store + "]"
	store = strings.ReplaceAll(store, ",]", "]")
	//fmt.Println(store)
	var (
		c          []LogData
		deployment string
	)

	json.Unmarshal([]byte(store), &c)
	switch {
	case len(c[0].Kubernetes.Labels.App) > 0:
		deployment = c[0].Kubernetes.Labels.App
	case len(c[0].Kubernetes.Labels.K8s_App) > 0:
		deployment = c[0].Kubernetes.Labels.K8s_App
	default:
		deployment = removeHash(c[0].Kubernetes.Pod)
		if len(deployment) == 0 {
			deployment = c[0].Kubernetes.Container
		}
	}

	for i := range c {
		fmt.Println(c[i].Message)
		u.upload(objectKey, c[i].Message+"\n", deployment, c[0].Kubernetes.Container)
	}
}
func removeHash(s string) string {
	var podName string
	for i := 0; i < len(s)-17; i++ {
		podName += s[i : i+1]
	}
	return podName
}

func (u *AzblobUploader) upload(objectKey string, b, deployment, k8sContainerName string) error {
	ctx, cred := authServicePrincipal()
	UNUSED(ctx)
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
	UNUSED(blobURL, options)
	blobWithDir := deployment + "/" + time.Now().Format("20060102") + "-" + k8sContainerName + ".log"
	blobContainer := strings.ToLower(os.Getenv("CLUSTER_NAME"))
	url := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", os.Getenv("STORAGE_ACCOUNT_NAME"), blobContainer, blobWithDir)
	appendBlobClient, err := az.NewAppendBlobClient(url, cred, nil)
	_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(b)), nil)
	if err != nil {
		_, err = appendBlobClient.Create(ctx, nil)
		if err != nil {
			logger.Printf("Failed to create new blob %s", err)
		}
		_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(b)), nil)
		if err != nil {
			logger.Fatalf("Failed to append the new Blob %s", err)
		}

	} else {
		logger.Printf("Successfully appended to existing blob %s")
	}

	// _, err = azblob.UploadBufferToBlockBlob(ctx, []byte(b), blobURL, options)
	// logger.Info(b)
	// if err != nil {
	// 	u.logger.Errorf("upload to blob error: %s", err.Error())
	// 	return err
	// }

	return nil
}

type Credentials struct {
	Client  string `envconfig:"AZURE_CLIENT_ID"`
	Secret  string `envconfig:"AZURE_CLIENT_SECRET"`
	Tenant  string `envconfig:"AZURE_TENANT_ID"`
	Subs    string `envconfig:"SUBSCRIPTION_ID"`
	Cluster string `envconfig:"CLUSTER_NAME"`
}

func authServicePrincipal() (context.Context, *azidentity.DefaultAzureCredential) {
	if !authEnvVars() {
		log.Fatalln("Error: Authentication environment variables not found")
	}
	ctx := context.Background()
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("Authentication Failed %s", err)
	}
	return ctx, cred
}

func authEnvVars() bool {
	var c Credentials
	err := envconfig.Process("Client", &c)
	if err != nil {
		log.Fatal(err.Error())
	}
	return true
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
func UNUSED(x ...interface{}) {}
