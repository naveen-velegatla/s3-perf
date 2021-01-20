package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func downlodObjects(noOfObj int) (fileSize int64) {
	cred := credentials.NewStaticCredentials(os.Getenv("ACCESSKEY"), os.Getenv("SECRETKEY"), "")
	sessionUp := session.New(aws.NewConfig().
		WithCredentials(cred).
		WithRegion("us-east-1").
		WithEndpoint(os.Getenv("ENDPOINT")).
		WithS3ForcePathStyle(true))

	downloader := s3manager.NewDownloader(sessionUp)

	for i := 0; i < noOfObj; i++ {
		var objectName string
		objectName = fmt.Sprintf("object%d", i+1)

		file,err := os.Create(objectName)
		if err != nil {
			log.Fatal(err)
		}
			
		numBytes, err1 := downloader.Download(file,
			&s3.GetObjectInput{
				Bucket: aws.String(os.Getenv("BUCKET")),
				Key:    aws.String(objectName),
			})
		if err1 != nil {
			fmt.Printf("Unable to download obj %q, %v", objectName, err1)
		}
		fileSize = numBytes
	}

	return fileSize;
}

func main() {
	//Objects can be populated for downloading using this script: 
	//https://github.com/minio/perftest/blob/master/parallel-upload-download/parallel-put.go
	
	noOfObj := os.Getenv("OBJECTCOUNT")
	nObj,err3 := strconv.Atoi(noOfObj)
	if err3 != nil {
		log.Fatalln(err3)
	}

	start := time.Now().UTC()
	fileSize := downlodObjects(nObj)
	elapsedTime := time.Since(start).Seconds()

	fmt.Printf("Elapsed time: %.2f Sec\n", elapsedTime)
	totalDownloadedSizeInMBit := (float64 (int64 (nObj) * fileSize * 8)  / 1024) / 1024
	fmt.Printf("Data Read/Downloaded: %.2f MBit/sec\n", totalDownloadedSizeInMBit/float64(elapsedTime))
}
