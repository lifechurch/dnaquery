package main

import (
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/urfave/cli"
)

func TestNewDNAQuery(t *testing.T) {
	_, err := NewDNAQuery(&Configuration{})
	if err == nil {
		t.Errorf("should error if 0 Containers in config")
	}
	c1 := Container{Name: "Container 1"}
	cfg := &Configuration{
		Containers: []Container{c1},
	}
	_, err = NewDNAQuery(cfg)
	if err == nil {
		t.Errorf("should error if no log directory is specified")
	}

	cfg, err = NewConfiguration("example.toml")
	dna, err := NewDNAQuery(cfg)
	if dna == nil {
		t.Error("dna sould not be nil")
	}

}

func TestCleanupFiles(t *testing.T) {
	// make a few files
	f1Path := "file1"
	f2Path := "file2"
	f3Path := "file3" // this file is never created
	f1, err := os.Create(f1Path)
	if err != nil {
		t.Fatalf("Couldn't create test file (%s): %s", f1Path, err.Error())
	}
	f1.Close()
	f2, err := os.Create(f2Path)
	if err != nil {
		t.Fatalf("Couldn't create test file (%s): %s", f1Path, err.Error())
	}
	f2.Close()
	cleanupFiles(f1Path, f2Path, f3Path)

	if _, err := os.Stat(f1Path); !os.IsNotExist(err) {
		t.Errorf("test file (%s) still exists", f1Path)
	}
	if _, err := os.Stat(f2Path); !os.IsNotExist(err) {
		t.Errorf("test file (%s) still exists", f2Path)
	}
}

func TestDNAQuery_readLine(t *testing.T) {
	ch := make(chan [2]string, 50)
	m := make(map[string]struct{})
	c1 := Container{
		Name:       "container1",
		Regex:      `^([\d.]+) \[([^\]]*)\] - "([^"]*)" (\d+)`,
		TimeGroup:  2,
		TimeFormat: "2/Jan/2006:15:04:05 -0700",
	}
	c2 := Container{
		Name:       "container2",
		Regex:      `^([\d.]+) - \[([^\]]*)\] - "([^"]*)" (\d+)`,
		TimeGroup:  2,
		TimeFormat: "2006-01-02T15:04:05-0700",
		Excludes:   []Exclude{{Group: 3, Contains: "ping"}},
	}
	// invalid configuration, should have code to detect this at start up, for now
	// make sure code handles it
	c3 := Container{
		Name:       "container3",
		Regex:      `^([\d.]+)`,
		TimeGroup:  2,
		TimeFormat: "2006-01-02T15:04:05-0700",
		Excludes:   []Exclude{{Group: 3, Contains: "ping"}},
	}
	cfg := &Configuration{
		Containers: []Container{c1, c2, c3},
		Storage:    Storage{LogDirectory: "/tmp/"},
	}
	type fields struct {
		Configuration  *Configuration
		containerNames map[string]struct{}
	}
	type args struct {
		path string
	}
	c := &cli.Context{}
	d := &DNAQuery{}
	dateToProcess := c.String("date")
	logName := d.getLogfile(dateToProcess)
	tests := []struct {
		name   string
		fields fields
		args   args
		want   chan [2]string
	}{
		// Test cases
		{
			name: "Test Log File",
			fields: fields{
				Configuration:  cfg,
				containerNames: m,
			},
			args: args{
				path: logName, // Need a path to a test Logfile here to readLines from
			},
			want: ch,
		},
	}
	close(ch)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DNAQuery{
				Configuration:  tt.fields.Configuration,
				containerNames: tt.fields.containerNames,
			}
			if got := d.readLine(tt.args.path); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DNAQuery.readLine() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProcessLine(t *testing.T) {
	c1 := Container{
		Name:       "container1",
		Regex:      `^([\d.]+) \[([^\]]*)\] - "([^"]*)" (\d+)`,
		TimeGroup:  2,
		TimeFormat: "2/Jan/2006:15:04:05 -0700",
	}
	c2 := Container{
		Name:       "container2",
		Regex:      `^([\d.]+) - \[([^\]]*)\] - "([^"]*)" (\d+)`,
		TimeGroup:  2,
		TimeFormat: "2006-01-02T15:04:05-0700",
		Excludes:   []Exclude{{Group: 3, Contains: "ping"}},
	}
	// invalid configuration, should have code to detect this at start up, for now
	// make sure code handles it
	c3 := Container{
		Name:       "container3",
		Regex:      `^([\d.]+)`,
		TimeGroup:  2,
		TimeFormat: "2006-01-02T15:04:05-0700",
		Excludes:   []Exclude{{Group: 3, Contains: "ping"}},
	}
	cfg := &Configuration{
		Containers: []Container{c1, c2, c3},
		Storage:    Storage{LogDirectory: "/tmp/"},
	}
	dna, err := NewDNAQuery(cfg)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	ch := make(chan [2]string)
	outfile := "output.csv"
	go dna.processLine(outfile, ch)
	// regular, expect normal operation
	ch <- [2]string{"container1", `123.123.123.123 [13/Nov/2017:13:23:01 -0000] - "GET view.json" 200`}
	ch <- [2]string{"container1", `123.123.123.123 [13/Nov/2017:13:23:04 -0000] - "GET ping.json" 200`}
	ch <- [2]string{"container2", `2.1.5.3 - [2017-12-03T13:23:04-0500] - "GET ping.json" 200`}
	ch <- [2]string{"container2", `2.1.5.3 - [2017-12-03T13:23:04-0500] - "GET view.json" 200`}
	// case where exclusion and time groups are more then regex
	ch <- [2]string{"container3", `2.1.5.3`}
	// case where there is no container registered
	ch <- [2]string{"container", `2.1.5.3 - [2017-12-03T13:23:04-0500] - "GET view.json" 200`}
	// case where the log line doesn't match regex
	ch <- [2]string{"container1", `error`}
	close(ch)

	// sleep a bit for goroutine to finish up once channel is closed
	time.Sleep(500 * time.Millisecond)
	dat, err := ioutil.ReadFile(outfile)
	if err != nil {
		t.Fatalf("unexpected error opening outfile, %v", err)
	}
	lines := strings.Split(string(dat), "\n")
	expectedLines := 5 // 4 logs + 1 empty line
	if len(lines) != expectedLines {
		t.Errorf("expected %d lines, found %d lines", expectedLines, len(lines))
	}
	cleanupFiles(outfile)
}
