package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
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
	toml "github.com/pelletier/go-toml"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"google.golang.org/api/option"
)

// AWS holds the configuration for [aws] section of the toml config.
type AWS struct {
	Key       string
	Secret    string
	Bucket    string
	LogPrefix string
}

// Storage holds the configuration for [storage] section of the toml config.
type Storage struct {
	LogDirectory string
}

// GCP holds the configuration for [gcp] section of the toml config.
type GCP struct {
	ProjectID       string
	CredentialsFile string
	Bucket          string
	Dataset         string
	TemplateTable   string
}

// Exclude holds the configuration for the [[containers.excludes]] subsection
// of the toml config.
type Exclude struct {
	Group    int
	Contains string
}

// Container holds the configuration for a single entry in the [[containers]]
// section of the toml config.
type Container struct {
	Name          string
	Regex         string
	CompiledRegex *regexp.Regexp
	TimeGroup     int
	TimeFormat    string
	Excludes      []Exclude
}

// Config holds the full configation loaded from the toml config file.
type Config struct {
	AWS        AWS
	Storage    Storage
	GCP        GCP
	Containers []Container
}

func (cfg *Config) getContainer(c string) (Container, error) {
	for _, container := range cfg.Containers {
		if c == container.Name {
			return container, nil
		}
	}
	return Container{}, errors.New("Container not found")
}

func (cfg *Config) compileRegexes() {
	for i, c := range cfg.Containers {
		cmp, err := regexp.Compile(c.Regex)
		if err != nil {
			log.Fatalln("Could not compile regex for container:", c.Name)
		}
		cfg.Containers[i].CompiledRegex = cmp
	}
}

func readConfig(path string, cfg *Config) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Unable to open config (%s)", path))
	}
	err = toml.Unmarshal(data, cfg)
	if err != nil {
		return errors.Wrap(err, "Error loading config")
	}
	return nil
}

func setupDirectory(cfg *Config) {
	err := os.MkdirAll(cfg.Storage.LogDirectory, os.ModePerm)
	if err != nil {
		log.Fatalf("Can't create log dir (%s): %s\n", cfg.Storage.LogDirectory, err.Error())
	}
}

func cleanupFiles(path ...string) {
	for _, f := range path {
		err := os.Remove(f)
		if err != nil {
			log.Printf("Unable to delete file (%s): %s", f, err.Error())
		}
	}
}

func readLine(path string, cfg *Config) chan [2]string {
	ch := make(chan [2]string, 50)
	go func(path string, ch chan [2]string, cfg *Config) {
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
			_, err := cfg.getContainer(container)
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
	}(path, ch, cfg)
	return ch
}

func processLine(path string, ch chan [2]string, cfg *Config) {
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
		c, err := cfg.getContainer(container)
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
		dt, err := time.Parse(c.TimeFormat, result[c.TimeGroup])
		if err != nil {
			// log.Println("skipping row:", err.Error())
			nSkipped++
			continue
		}
		result[c.TimeGroup] = dt.Format("2006-01-02 15:04:05 -07:00")
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

func getLogfile(cfg *Config, logDate string) (logName string) {
	d, _ := time.Parse("2006-01-02", logDate)
	logName = cfg.AWS.LogPrefix + "." + logDate + ".json.gz"
	localLogName := filepath.Join(cfg.Storage.LogDirectory, logName)
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
	ob, err := svc.GetObject(obi)
	if err != nil {
		log.Fatal("Error getting object:", err.Error())
	}
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

func uploadToGCS(path string, object string, cfg *Config) error {
	log.Println("Starting upload to GCS")
	os.Setenv("GOOGLE_CLOUD_PROJECT", cfg.GCP.ProjectID)
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(cfg.GCP.CredentialsFile))
	if err != nil {
		log.Fatalln("Error creating storage client", err.Error())
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, _ := f.Stat()
	log.Printf("Upload size: %f MB\n", float64(stat.Size())/1024/1024)
	wc := client.Bucket(cfg.GCP.Bucket).Object(object).NewWriter(ctx)
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

func loadInBQ(object string, date string, cfg *Config) {
	date = strings.Replace(date, "-", "_", -1)
	log.Println("Starting load into BQ")
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, cfg.GCP.ProjectID,
		option.WithCredentialsFile(cfg.GCP.CredentialsFile))
	if err != nil {
		log.Fatalln("Error creating BQ Client", err.Error())

	}
	myDataset := client.Dataset(cfg.GCP.Dataset)

	templateTable := myDataset.Table(cfg.GCP.TemplateTable)

	gscURL := "gs://" + cfg.GCP.Bucket + "/" + object
	gcsRef := bigquery.NewGCSReference(gscURL)
	tmpTableMeta, err := templateTable.Metadata(ctx)
	if err != nil {
		log.Fatalln("Error getting template table", err.Error())
	}
	gcsRef.Schema = tmpTableMeta.Schema
	tableName := date
	loader := myDataset.Table(tableName).LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateIfNeeded
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	if err != nil {
		log.Fatalln("Error running job:", err.Error())
	}
	log.Println("BQ Job created...")
	pollInterval := 5 * time.Second
	for {
		status, err := job.Status(ctx)
		if err != nil {
			log.Fatalln("Job Error:", err.Error())
		}
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
	cfg := &Config{}
	err := readConfig(configFile, cfg)
	if err != nil {
		log.Fatalln(err.Error())
	}
	cfg.compileRegexes()

	setupDirectory(cfg)

	logName := getLogfile(cfg, dateToProcess)

	ch := readLine(logName, cfg)

	outFile := "results_" + dateToProcess + ".csv"
	outPath := filepath.Join(cfg.Storage.LogDirectory, outFile)
	processLine(outPath, ch, cfg)

	gcsObject := dateToProcess + "_results.csv"
	err = uploadToGCS(outPath, gcsObject, cfg)
	if err != nil {
		log.Fatalln("Error uploading to GCS:", err.Error())
	}
	loadInBQ(gcsObject, dateToProcess, cfg)

	cleanupFiles(logName, outPath)
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
