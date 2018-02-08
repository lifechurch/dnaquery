package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"google.golang.org/api/option"
)

type DNAQuery struct {
	cfg *Configuration
}

func NewDNAQuery(cfg *Configuration) (*DNAQuery, error) {
	if len(cfg.Containers) < 1 {
		return nil, errors.New("Configuration needs at least 1 container")
	}
	dna := &DNAQuery{
		cfg: cfg,
	}
	dna.cfg.compileRegex()
	err := dna.cfg.setupDirectory()
	if err != nil {
		return nil, errors.Wrap(err, "Error setting up directory")
	}
	return dna, nil
}

func cleanupFiles(path ...string) {
	for _, f := range path {
		err := os.Remove(f)
		if err != nil {
			log.Printf("Unable to delete file (%s): %s", f, err.Error())
		}
	}
}

func (d *DNAQuery) readLine(path string) chan [2]string {
	ch := make(chan [2]string, 50)
	go func(path string, ch chan [2]string) {
		defer close(ch)
		log.Println("Opening Logfile", path)
		inFile, err := os.Open(path)
		if err != nil {
			log.Fatalf("Unable to open input: %s", path)
		}
		gz, err := gzip.NewReader(inFile)

		if err != nil {
			log.Fatal(err)
		}
		defer inFile.Close()
		defer gz.Close()

		scanner := bufio.NewScanner(gz)
		log.Println("Scanning log file")
		lineCount := 0
		for scanner.Scan() {
			lineCount++
			data := scanner.Bytes()
			container, _ := jsonparser.GetString(data, "container")
			_, err := d.cfg.getContainer(container)
			if err != nil {
				continue
			}
			line, _ := jsonparser.GetString(data, "_line")
			ch <- [2]string{container, line}
		}
		if err := scanner.Err(); err != nil {
			log.Fatalln("Error reading log file:", err.Error())
		}
		log.Printf("Scanning complete. %d lines scanned\n", lineCount)
	}(path, ch)
	return ch
}

func (d *DNAQuery) processLine(path string, ch chan [2]string) {
	log.Println("Starting processLine")
	csvOut, err := os.Create(path)
	if err != nil {
		log.Fatalf("Unable to open output: %s", path)
	}
	w := csv.NewWriter(csvOut)
	defer csvOut.Close()
	nMatches := 0
	nSkipped := 0
	for r := range ch {
		container := r[0]
		line := r[1]
		var record []string
		record = append(record, container)
		c, err := d.cfg.getContainer(container)
		if err != nil {
			log.Println("Can't find container config in processLine:", container)
			continue
		}
		result := c.CompiledRegex.FindStringSubmatch(line)
		if len(result) == 0 {
			// log.Println("Found no match for regex in processLine:", container, line)
			nSkipped++
			continue
		}
		// check exclusion rules
		exclude := false
		for _, e := range c.Excludes {
			if e.Group >= len(result) {
				log.Printf("skipping exclusion: %v, Group not found in result", e)
				continue
			}
			if strings.Contains(result[e.Group], e.Contains) {
				exclude = true
				break
			}
		}
		if exclude {
			nSkipped++
			continue
		}
		// change time format
		if c.TimeGroup >= len(result) {
			log.Println("skipping time formating. Group not found in result")
		} else {
			dt, err := time.Parse(c.TimeFormat, result[c.TimeGroup])
			if err != nil {
				// log.Println("skipping row:", err.Error())
				nSkipped++
				continue
			}
			result[c.TimeGroup] = dt.Format("2006-01-02 15:04:05 -07:00")
		}
		nMatches++
		record = append(record, result[1:]...)
		if err = w.Write(record); err != nil {
			log.Fatal(err)
		}
	}
	// Write any buffered data.
	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatalln("Error creates csv", err.Error())
	}
	log.Printf("Matched %d lines, Skipped %d lines\n", nMatches, nSkipped)
	log.Println("Completed processLine")
}

func (d *DNAQuery) getLogfile(logDate string) (logName string) {
	date, _ := time.Parse("2006-01-02", logDate)
	logName = d.cfg.AWS.LogPrefix + "." + logDate + ".json.gz"
	localLogName := filepath.Join(d.cfg.Storage.LogDirectory, logName)
	item := date.Format("2006/01/") + logName

	awsCfg := &aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(d.cfg.AWS.Key, d.cfg.AWS.Secret, ""),
	}
	sess, err := session.NewSession(awsCfg)
	CheckErr("Unable to create session: ", err)

	svc := s3.New(sess)
	obi := &s3.GetObjectInput{
		Bucket: aws.String(d.cfg.AWS.Bucket),
		Key:    aws.String(item),
	}
	ob, err := svc.GetObject(obi)
	CheckErr("Error getting object: ", err)
	sizeInS3 := *ob.ContentLength
	log.Printf("File in S3 is %f GB", float64(*ob.ContentLength)/1024/1024/1024)

	downloadFile := true
	if file, err := os.Open(localLogName); err == nil {
		stat, _ := file.Stat()
		// if file on disk is same size don't download
		downloadFile = stat.Size() != sizeInS3
		file.Close()
	}

	if downloadFile {
		log.Printf("Downloading from S3")
		downloader := s3manager.NewDownloader(sess)

		file, err := os.Create(localLogName)
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
	return localLogName
}

func (d *DNAQuery) uploadToGCS(path string, object string) error {
	log.Println("Starting upload to GCS")
	os.Setenv("GOOGLE_CLOUD_PROJECT", d.cfg.GCP.ProjectID)
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(d.cfg.GCP.CredentialsFile))
	CheckErr("Error creating storage client: ", err)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, _ := f.Stat()
	log.Printf("Upload size: %f MB\n", float64(stat.Size())/1024/1024)
	wc := client.Bucket(d.cfg.GCP.Bucket).Object(object).NewWriter(ctx)
	nBytes, nChunks := int64(0), int64(0)
	r := bufio.NewReader(f)
	buf := make([]byte, 0, 4*1024)
	for {
		n, err := r.Read(buf[:cap(buf)])
		buf = buf[:n]
		if n == 0 {
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		nChunks++
		nBytes += int64(len(buf))
		_, err = wc.Write(buf)
		if err != nil {
			return err
		}
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
	}

	if err := wc.Close(); err != nil {
		return err
	}
	if err := client.Close(); err != nil {
		log.Fatal("Error closing GCS:", err.Error())
	}
	log.Println("Completed upload to GCS")
	return nil
}

func (d *DNAQuery) loadInBQ(object string, date string) {
	date = strings.Replace(date, "-", "_", -1)
	log.Println("Starting load into BQ")
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, d.cfg.GCP.ProjectID,
		option.WithCredentialsFile(d.cfg.GCP.CredentialsFile))
	CheckErr("Error creating BQ Client: ", err)

	myDataset := client.Dataset(d.cfg.GCP.Dataset)

	templateTable := myDataset.Table(d.cfg.GCP.TemplateTable)

	gscURL := "gs://" + d.cfg.GCP.Bucket + "/" + object
	gcsRef := bigquery.NewGCSReference(gscURL)
	tmpTableMeta, err := templateTable.Metadata(ctx)
	CheckErr("Error getting template table: ", err)
	gcsRef.Schema = tmpTableMeta.Schema
	tableName := date
	loader := myDataset.Table(tableName).LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateIfNeeded
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	CheckErr("Error running job: ", err)
	log.Println("BQ Job created...")
	pollInterval := 5 * time.Second
	for {
		status, err := job.Status(ctx)
		CheckErr("Job Error: ", err)
		if status.Done() {
			if status.Err() != nil {
				log.Fatalf("Job failed with error %v", status.Err())
			}
			break
		}
		time.Sleep(pollInterval)
	}
	log.Println("Completed load into BQ")
}

func run(c *cli.Context) error {
	dateToProcess := c.String("date")
	if dateToProcess == "" {
		fmt.Print("ERROR: --date required\n\n")
		cli.ShowAppHelp(c)
		return errors.New("Error: --date required")
	}
	configFile := c.String("config")

	// load config
	cfg, err := NewConfiguration(configFile)
	CheckErr("Error: ", err)

	dna, err := NewDNAQuery(cfg)
	CheckErr("Error: ", err)

	logName := dna.getLogfile(dateToProcess)
	defer cleanupFiles(logName)

	ch := dna.readLine(logName)

	outFile := "results_" + dateToProcess + ".csv"
	outPath := filepath.Join(cfg.Storage.LogDirectory, outFile)
	defer cleanupFiles(outPath)
	dna.processLine(outPath, ch)

	gcsObject := dateToProcess + "_results.csv"
	err = dna.uploadToGCS(outPath, gcsObject)
	CheckErr("Error uploading to GCS:", err)

	dna.loadInBQ(gcsObject, dateToProcess)

	return nil
}

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Usage: "Load configuration from `FILE`",
			Value: "dnaquery.toml",
		},
		cli.StringFlag{
			Name:  "date, d",
			Usage: "process log archive for `YYYY-MM-DD`",
		},
	}
	app.Action = run
	app.Run(os.Args)
}
