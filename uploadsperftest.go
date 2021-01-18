package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	// constant for default random seed.
	defaultRandomSeed = 42

	// minimum per worker upload count
	minUploadCount = 10

	// minimum per worker download count
	minDownloadCount = 10

	// maximum number of distinct objects
	maxDistinctObjects = 100000
)

var (
	alNum = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	// represent parent dirs when spaces are replaced by path
	// separators
	parentDirs = []string{
		"It is certain",
		"It is decidedly so",
		"Without a doubt",
		"Yes definitely",
		"You may rely on it",
		"As I see it yes",
		"Most likely",
		"Outlook good",
		"Yes",
		"Signs point to yes",
		"Reply hazy try again",
		"Ask again later",
		"Better not tell you now",
		"Cannot predict now",
		"Concentrate and ask again",
		"Don't count on it",
		"My reply is no",
		"My sources say no",
		"Outlook not so good",
		"Very doubtful",
	}

	// Read settings from environment
	accessKey = os.Getenv("ACCESS_KEY")
	secretKey = os.Getenv("SECRET_KEY")

	// settings from command line
	endpoint       string
	secure         bool
	bucket         string
	concurrency    int
	randomSeed     int64
	maxDiskUsageGB int

	// max number of distinct object names.
	maxObjCount int

	totalTime int
	interval int

	// random object names
	randObjNames []string
)

type devNull int
func (devNull) WriteAt(p []byte, off int64) (int, error) {
	return len(p), nil
}


func generateNames() {
	fmt.Println("Generating names for objects...")
	randObjNames = make([]string, 0, maxObjCount)
	for i := 0; i < maxObjCount; i++ {
		randObjNames = append(randObjNames, getRandomObjectName())
	}
	fmt.Println("done.")
}

func setMaxObjects(size int64) {
	maxDiskUsage := int64(maxDiskUsageGB) * 1000 * 1000 * 1000
	maxObjCount = maxDistinctObjects
	ratio := maxDiskUsage / size
	if ratio < int64(maxObjCount) {
		maxObjCount = int(ratio)
	}
}

func getAlNumPerm() string {
	n := len(alNum)
	p := rand.Perm(n)
	objNameRunes := make([]rune, n)
	for i := 0; i < n; i++ {
		objNameRunes[i] = alNum[p[i]]
	}
	return string(objNameRunes)
}

func getRandomObjectName() string {
	dirString := parentDirs[rand.Intn(len(parentDirs))]
	objPath := filepath.Join(strings.Fields(dirString)...)

	rnum := rand.Intn(1000000000)
	n := fmt.Sprintf("%v%v%v", rnum, rnum, rnum)

	return filepath.Join(objPath, n)
}

// object generator type - generates object content without IO.
type ObjGen struct {
	// name of object
	ObjectName string

	// size in KiB
	ObjectSize int64

	// seed string that repeats inside the object
	SeedBytes []byte

	// index to read at in the whole logical object
	readIndex int64
}

func NewRandomObjectWithSize(size int64) ObjGen {
	return ObjGen{
		ObjectName: randObjNames[rand.Intn(len(randObjNames))],
		ObjectSize: size,
		SeedBytes:  []byte(getAlNumPerm()),
	}
}

// implement Reader interface
func (og *ObjGen) Read(p []byte) (n int, err error) {
	for n < len(p) && og.readIndex < og.ObjectSize {
		bufIxStart := og.readIndex % int64(len(og.SeedBytes))
		bytesLeftInObject := og.ObjectSize - og.readIndex
		bytesLeftInSeedBytes := int64(len(og.SeedBytes)) - bufIxStart
		var wroteCount int
		if bytesLeftInObject < bytesLeftInSeedBytes {
			wroteCount = copy(p[n:],
				og.SeedBytes[bufIxStart:bufIxStart+bytesLeftInObject])
		} else {
			wroteCount = copy(p[n:],
				og.SeedBytes[bufIxStart:])
		}
		n += wroteCount
		og.readIndex += int64(wroteCount)
	}
	if og.readIndex >= og.ObjectSize {
		err = io.EOF
	}
	return
}

func (og *ObjGen) Seek(off int64, whence int) (int64, error) {
	var nval int64
	switch whence {
	case io.SeekStart:
		nval = off
	case io.SeekCurrent:
		nval = og.readIndex + off
	case io.SeekEnd:
		nval = og.ObjectSize - off
	}
	if nval < 0 || nval > og.ObjectSize {
		return 0, errors.New("invalid seek offset")
	}
	og.readIndex = nval
	return og.readIndex, nil
}

// returns length of object
func (og *ObjGen) Size() int64 {
	return og.ObjectSize
}

// Returns number of bytes expressed by human friendly
// string. Supports:
//
// 1. Raw byte number ("124")
// 2. Number with unit (no intervening whitespace).
//
// Supported units: KB, MB, GB, TB, KiB, MiB, GiB and TiB.
func parseHumanNumber(s string) (int64, error) {
	multiplier := []int64{
		1000,
		1000 * 1000,
		1000 * 1000 * 1000,
		1000 * 1000 * 1000 * 1000,
		1024,
		1024 * 1024,
		1024 * 1024 * 1024,
		1024 * 1024 * 1024 * 1024,
	}
	suffixes := []string{
		"KB", "MB", "GB", "TB",
		"KiB", "MiB", "GiB", "TiB",
	}
	badSizeErr := errors.New("invalid size number given")
	for i, suffix := range suffixes {
		if strings.HasSuffix(s, suffix) {
			v := strings.TrimSuffix(s, suffix)
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return 0, badSizeErr
			}
			return n * multiplier[i], nil
		}
	}
	// try to parse raw byte number
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, badSizeErr
	}
	return n, nil
}

func getAWSSession() (*session.Session, error) {
	return session.NewSessionWithOptions(
		session.Options{
			Config: aws.Config{
				Endpoint: aws.String(endpoint),
				Region:   aws.String("us-east-1"),
				Credentials: credentials.NewStaticCredentials(
					accessKey, secretKey, ""),
				DisableSSL:       aws.Bool(true),
				S3ForcePathStyle: aws.Bool(true)},
		},
	)
}

var (
	errWorkerSucc = errors.New("Worker is exiting with success.")
	errWorkerQuit = errors.New("Worker is quitting due to quit signal.")
)

type workerMsg struct {
	// If exitingErr != nil -> worker is quitting with an error
	// value.
	exitingErr error
	objectName string

	// Sends time at which putobject was successful
	putStartTime time.Time
	putDuration  time.Duration
}


func downloadWorker(objNameList []string, workerMsgCh chan <- workerMsg, quitChan <- chan struct{}){
	session, err := getAWSSession()
	if err != nil{
		workerMsgCh <- workerMsg{exitingErr: err}
		return
	}

	s3Downloader := s3manager.NewDownloader(session)
	var Discard io.WriterAt = devNull(0)
	objDownloader := func(doneCh chan<- workerMsg, objName string) {
		startTime := time.Now().UTC()
		_, err = s3Downloader.Download(Discard, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(objName),
		})
		duration := time.Since(startTime)
		if err != nil {
			err = fmt.Errorf("GetObject Error from bucket %v and key %v - %v", bucket, objName, err)
		}
		doneCh <- workerMsg{err, objName, startTime, duration}
	}
	// buffered channel so that downloader go routine does not hang.
	doneCh := make(chan workerMsg, 1)
	downloadCount := 0
	timeStart := time.Now().UTC()
	i := 0
	go objDownloader(doneCh, objNameList[i])
	toQuit := false
	// minimum worker running time
	workerDuration := time.Duration(time.Minute * time.Duration(totalTime))
	for !toQuit {
		select {
		case downloadMsg := <-doneCh:
			workerMsgCh <- downloadMsg
			if downloadMsg.exitingErr != nil {
				toQuit = true
			} else {
				downloadCount++
				if time.Since(timeStart) < workerDuration ||
					downloadCount < minDownloadCount {
					if i == len(objNameList){
						i = 0
					}
					go objDownloader(doneCh, objNameList[i])
				} else {
					workerMsgCh <- workerMsg{
						exitingErr: errWorkerSucc,
					}
					toQuit = true
				}
			}
		case <-quitChan:
			workerMsgCh <- workerMsg{
				exitingErr: errWorkerQuit,
			}
			toQuit = true
		}
	}

}

func uploadWorker(objSize int64, workerMsgCh chan<- workerMsg, quitChan <-chan struct{}) {
	session, err := getAWSSession()
	if err != nil {
		workerMsgCh <- workerMsg{exitingErr: err}
		return
	}

	uploader := func(doneCh chan<- workerMsg) {
		object := NewRandomObjectWithSize(objSize)
		startTime := time.Now().UTC()

		s3Client := s3.New(session)

		_, err = s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object.ObjectName),
			Body:   &object,
		})
		duration := time.Since(startTime)
		if err != nil {
			err = fmt.Errorf("PutObject Error for bucket %v and key %v - %v", bucket, object.ObjectName, err)
		}
		doneCh <- workerMsg{err, object.ObjectName, startTime, duration}
	}

	// buffered channel so that uploader go routine does not hang.
	doneCh := make(chan workerMsg, 1)
	uploadCount := 0
	timeStart := time.Now().UTC()
	go uploader(doneCh)
	// minimum worker running time
	workerDuration := time.Duration(time.Minute * time.Duration(totalTime))
	toQuit := false
	for !toQuit {
		select {
		case uploadMsg := <-doneCh:
			workerMsgCh <- uploadMsg
			if uploadMsg.exitingErr != nil {
				toQuit = true
			} else {
				uploadCount++
				if time.Since(timeStart) < workerDuration ||
					uploadCount < minUploadCount {
					go uploader(doneCh)
				} else {
					workerMsgCh <- workerMsg{
						exitingErr: errWorkerSucc,
					}
					toQuit = true
				}
			}
		case <-quitChan:
			workerMsgCh <- workerMsg{
				exitingErr: errWorkerQuit,
			}
			toQuit = true
		}
	}
}

type TestResult struct {
	// putStartTime []time.Time
	// putDuration  []time.Duration

	startTime   time.Time
	objectSize  int64
	objectCount int64
}

func (tr *TestResult) getTRMessage(upload uint) string {
	timeSoFar := time.Now().UTC().Sub(tr.startTime).Seconds()
	bandwidthMiBps := float64(tr.objectCount) * float64(tr.objectSize) /
		(timeSoFar * 1024 * 1024)
	objps := float64(tr.objectCount) / timeSoFar

	totalDataMiB := float64(tr.objectCount*tr.objectSize) /
		float64(1024*1024)
	action := "Written"
	if upload == 0{
		action = "Downloaded"
	}

	return fmt.Sprintf("At %.2f: Avg data b/w: %.2f MiBps. Avg obj/s: %.2f. Data %s: %0.2f MiB in %v objects.\n", timeSoFar,
		 bandwidthMiBps, objps, action, totalDataMiB, tr.objectCount) }

func printRoutine(msgCh chan string, printerDoneCh chan struct{}) {
	for msg := range msgCh {
		fmt.Print(msg)
	}
	printerDoneCh <- struct{}{}
}

func downloadLaunchTest(objSize int64, objNames []string) (tr TestResult, err error){
	workerMsgCh := make(chan workerMsg)
	// channels to print asynch.
	// buffer upto 100 messages
	printMsgCh := make(chan string, 100)
	printerDoneCh := make(chan struct{})
	go printRoutine(printMsgCh, printerDoneCh)
	fmt.Println(concurrency)
	quitCh := make(chan struct{}, concurrency)
	noOfObjs := len(objNames)
	objsPerThread := noOfObjs / concurrency
	start := 0
	end := objsPerThread
	for i := 0; i < concurrency; i++ {
		go downloadWorker(objNames[start:end], workerMsgCh, quitCh)
		start = end
		end = end + objsPerThread
	}



	// collect results and wait for workers to quit.
	numWorkersQuit := 0
	isQuitting := false
	eachInterval := time.After(time.Second * time.Duration(interval))
	tr.startTime = time.Now().UTC()
	tr.objectSize = objSize
	var hadDownloadError error
	for numWorkersQuit < concurrency {
		select {
		case wMsg := <-workerMsgCh:
			switch {
			case wMsg.exitingErr == errWorkerSucc:
				fallthrough
			case wMsg.exitingErr == errWorkerQuit:
				numWorkersQuit++
			case wMsg.exitingErr != nil:
				fmt.Printf("An download attempt errored with \"%v\" - aborting test!\n", wMsg.exitingErr)
				hadDownloadError = wMsg.exitingErr
				numWorkersQuit++
				if !isQuitting {
					isQuitting = true
					for i := 0; i < concurrency; i++ {
						quitCh <- struct{}{}
					}
				}
			default:
				// got a successful upload msg.
				// tRes.putStartTime = append(tRes.putStartTime, wMsg.putStartTime)
				// tRes.putDuration = append(tRes.putDuration, wMsg.putDuration)

				tr.objectCount++
			}

		// print messages about the running test each second.
		case <-eachInterval:
			// print via a separate go routine so as to
			// not block the for loop for printing.
			printMsgCh <- tr.getTRMessage(0)
			eachInterval = time.After(time.Second * time.Duration(interval))
		}
	}

	// Close and confirm the printing channel exits.
	close(printMsgCh)
	<-printerDoneCh

	return tr, hadDownloadError

}

func uploadLaunchTest(objSize int64) (tr TestResult, objNameList []string, err error) {
	setMaxObjects(objSize)
	generateNames()

	// try to create bucket in case it doesnt exist.
	session, err := getAWSSession()
	if err != nil {
		return TestResult{}, objNameList, err
	}
	s3Client := s3.New(session)

	// ignore error as it is most likely that the bucket exists.
	_, _ = s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})

	workerMsgCh := make(chan workerMsg)

	// channels to print asynch.
	// buffer upto 100 messages
	printMsgCh := make(chan string, 100)
	printerDoneCh := make(chan struct{})
	go printRoutine(printMsgCh, printerDoneCh)

	// quitCh is buffered as some workers may have quit due to
	// errors when we send the quit signal.
	quitCh := make(chan struct{}, concurrency)

	// Start workers
	for i := 0; i < concurrency; i++ {
		go uploadWorker(objSize, workerMsgCh, quitCh)
	}

	// collect results and wait for workers to quit.
	numWorkersQuit := 0
	isQuitting := false
	tr.objectSize = objSize
	var hadUploadError error
	tr.startTime = time.Now().UTC()
	eachInterval := time.After(time.Second * time.Duration(interval))
	for numWorkersQuit < concurrency {
		select {
		case wMsg := <-workerMsgCh:
			switch {
			case wMsg.exitingErr == errWorkerSucc:
				fallthrough
			case wMsg.exitingErr == errWorkerQuit:
				numWorkersQuit++
			case wMsg.exitingErr != nil:
				fmt.Printf("An upload attempt errored with \"%v\" - aborting test!\n", wMsg.exitingErr)
				hadUploadError = wMsg.exitingErr
				numWorkersQuit++
				if !isQuitting {
					isQuitting = true
					for i := 0; i < concurrency; i++ {
						quitCh <- struct{}{}
					}
				}
			default:
				// got a successful upload msg.
				// tRes.putStartTime = append(tRes.putStartTime, wMsg.putStartTime)
				// tRes.putDuration = append(tRes.putDuration, wMsg.putDuration)
				tr.objectCount++
				objNameList = append(objNameList, wMsg.objectName)
			}

		// print messages about the running test each second.
		case <-eachInterval:
			// print via a separate go routine so as to
			// not block the for loop for printing.
			printMsgCh <- tr.getTRMessage(1)
			eachInterval = time.After(time.Second * time.Duration(interval))
		}
	}

	// Close and confirm the printing channel exits.
	close(printMsgCh)
	<-printerDoneCh

	return tr, objNameList, hadUploadError
}

/*

Worker Algo:

1. Generate objects of specified size, and upload sequentially to
service.

2. Report each success via a channel

3. Terminate on:
   a. Error, or
   b. 15 minutes pass and at least 50 objects are uploaded.
   c. Receiving signal to quit.

In the main thread, setup required number of worker threads, and:

1. Record each success and calculate objects/second metric.

2. On error, signal all workers to quit.

3. Wait for threads to quit, and report metrics.

*/

func init() {
	flag.StringVar(&endpoint, "h", "localhost:9000", "service endpoint host")
	flag.BoolVar(&secure, "s", false, "Set if endpoint requires https")
	flag.StringVar(&bucket, "bucket", "bucket", "Bucket to use for uploads test")
	flag.IntVar(&concurrency, "c", 1, "concurrency - number of parallel uploads")
	flag.Int64Var(&randomSeed, "seed", defaultRandomSeed, "random seed")
	flag.IntVar(&maxDiskUsageGB, "m", 80, "Maximum amount of disk usage in GBs")
	flag.IntVar(&interval, "i", 10, "interval time in seconds")
	flag.IntVar(&totalTime, "t", 15, "time for uploading/downloading")
}

func main() {
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Println("Usage: ./minio-perftest [flags] UPLOADS_SIZE")
		os.Exit(1)
	}

	// parse command line argument
	size, err := parseHumanNumber(flag.Arg(0))
	if err != nil {
		fmt.Println("Usage: ./minio-perftest [flags] UPLOADS_SIZE")
		fmt.Println("\nUPLOADS_SIZE examples: 100, 1MB, 10KiB, etc")
		os.Exit(1)
	}

	// set random seed for this run
	rand.Seed(randomSeed)

	// launch test
	fmt.Println("# Uploading Perf Stats ")
	result, objNames, err := uploadLaunchTest(size)
	if err != nil {
		fmt.Println("Quit due to errors:", err)
		os.Exit(1)
	}
	fmt.Print(result.getTRMessage(1))
	fmt.Println("# Downloading Perf Stats")
	result, err = downloadLaunchTest(size, objNames)
	if err != nil {
		fmt.Println("Quit due to errors:", err)
		os.Exit(1)
	}
	fmt.Print(result.getTRMessage(0))



}
