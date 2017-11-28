package main

import (
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestReadConfig(t *testing.T) {
	cfg := &Config{}
	err := readConfig("doesnotexist.toml", cfg)
	if err == nil {
		t.Error("should have returned an error")
	}
	err = readConfig("example.toml", cfg)
	if err != nil {
		t.Error("could not parse toml file when should have", err.Error())
	}
}

func TestGetContainers(t *testing.T) {
	// build a config with 2 Containers
	c1 := Container{Name: "Container 1"}
	cfg := &Config{
		Containers: []Container{c1},
	}
	_, err := cfg.getContainer("Container That Doesn't Exist")
	if err == nil {
		t.Errorf("Err not returned when container doesn't exist")
	}

	c1Retrieved, _ := cfg.getContainer("Container 1")
	if !cmp.Equal(c1, c1Retrieved) {
		t.Errorf("c1 was not equal")
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
		t.Fatalf("test file (%s) still exists", f1Path)
	}
	if _, err := os.Stat(f2Path); !os.IsNotExist(err) {
		t.Fatalf("test file (%s) still exists", f2Path)
	}
}
