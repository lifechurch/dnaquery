package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/buger/jsonparser"
	toml "github.com/pelletier/go-toml"
)

type AWS struct {
	Key       string
	Secret    string
	Bucket    string
	LogPrefix string
}
type Config struct {
	AWS AWS
}

func readConfig(path string, cfg *Config) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("Unable to open config %s: Error: %s", path, err.Error())
	}
	toml.Unmarshal(data, cfg)
}

func readLine(path string) chan string {
	ch := make(chan string, 50)
	go func(path string, ch chan string) {
		defer close(ch)
		inFile, err := os.Open(path)
		if err != nil {
			log.Fatalf("Unable to open input: %s", path)
		}
		defer inFile.Close()
		scanner := bufio.NewScanner(inFile)
		scanner.Split(bufio.ScanLines)

		for scanner.Scan() {
			data := scanner.Bytes()
			container, _ := jsonparser.GetString(data, "container")
			if container != "plans-app-production" {
				continue
			}
			line, _ := jsonparser.GetString(data, "_line")
			ch <- line
		}
	}(path, ch)
	return ch
}

func processLine(path string, ch chan string) {
	csvOut, err := os.Create(path)
	if err != nil {
		log.Fatalf("Unable to open output: %s", path)
	}
	w := csv.NewWriter(csvOut)
	defer csvOut.Close()
	for l := range ch {
		rec := strings.Split(l, " ")
		if err = w.Write(rec); err != nil {
			log.Fatal(err)
		}
	}
}

func getLogfile(cfg *Config, logDate string) (logName string) {
	d, _ := time.Parse("2006-01-02", logDate)
	logName = cfg.AWS.LogPrefix + "." + logDate + ".json.gz"
	item := d.Format("2006/01/") + logName

	awsCfg := &aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(cfg.AWS.Key, cfg.AWS.Secret, ""),
	}
	sess, err := session.NewSession(awsCfg)
	if err != nil {
		log.Fatalf("Unable to create session: %v", err)
	}

	svc := s3.New(sess)
	obi := &s3.GetObjectInput{
		Bucket: aws.String(cfg.AWS.Bucket),
		Key:    aws.String(item),
	}
	ob, _ := svc.GetObject(obi)
	sizeInS3 := *ob.ContentLength
	log.Printf("File in S3 is %f GB", float64(*ob.ContentLength)/1024/1024/1024)

	downloadFile := true
	if file, err := os.Open(logName); err == nil {
		stat, _ := file.Stat()
		// if file on disk is same size don't download
		downloadFile = stat.Size() != sizeInS3
		file.Close()
	}

	if downloadFile {
		log.Printf("Downloading from S3")
		downloader := s3manager.NewDownloader(sess)

		file, err := os.Create(logName)
		if err != nil {
			log.Fatalf("Unable to open file %q, %v", item, err)
		}
		defer file.Close()

		numBytes, err := downloader.Download(file, obi)

		if err != nil {
			log.Fatalf("Unable to download item %q, %v", item, err)
		}
		fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
	} else {
		log.Printf("Skipping Download")
	}
	return logName
}

func main() {
	cfg := &Config{}
	readConfig("dnaquery.toml", cfg)

	logName := getLogfile(cfg, "2017-08-06")
	fmt.Println(logName)

	// if len(os.Args) != 3 {
	// 	fmt.Printf("Usage: %s infile outfile\n", os.Args[0])
	// 	return
	// }
	// inFile := os.Args[1]
	// outFile := os.Args[2]
	// ch := readLine(inFile)
	// processLine(outFile, ch)
}
