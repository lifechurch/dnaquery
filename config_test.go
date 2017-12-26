package main

import (
	"crypto/rand"
	"os"
	"testing"

	"encoding/hex"
	"path/filepath"

	"github.com/google/go-cmp/cmp"
)

func TestReadConfig(t *testing.T) {
	_, err := NewConfiguration("doesnotexist.toml")

	if err == nil {
		t.Error("should have returned an error")
	}
	_, err = NewConfiguration("example.toml")
	if err != nil {
		t.Error("could not parse toml file when should have", err.Error())
	}
	// create bad file
	f := "bad.toml"
	f1, err := os.Create(f)
	f1.Write([]byte("bad toml file"))
	f1.Close()
	_, err = NewConfiguration(f)
	if err == nil {
		t.Errorf("should have error reading bad toml file")
	}
	// clean up test file
	os.Remove(f)
}

func TestGetContainers(t *testing.T) {
	// build a config with 1 Containers
	c1 := Container{Name: "Container 1"}
	cfg := &Configuration{
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


func TestCompileRegex(t *testing.T) {
	c1 := Container{
		Name:  "Container 1",
		Regex: `([\d.]+) - \[([^\]]*)\] - - \[([^\]]*)\]`,
	}
	cfg := &Configuration{
		Containers: []Container{c1},
	}
	if cfg.Containers[0].CompiledRegex != nil {
		t.Errorf("compile regex is not nil but compile hasn't been called yet")
	}
	cfg.compileRegex()
	if cfg.Containers[0].CompiledRegex == nil {
		t.Errorf("compile regex is nil")
	}
}

func TestSetupDir(t *testing.T) {
	randBytes := make([]byte, 16)
	rand.Read(randBytes)

	path := filepath.Join(os.TempDir(), hex.EncodeToString(randBytes))
	cfg := &Configuration{
		Storage: Storage{LogDirectory: path},
	}
	err := cfg.setupDirectory()
	if err != nil {
		t.Errorf("directory setup failed: %v", err)
	}
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		t.Errorf("directory does not exist: %v", err)
	}
	os.Remove(path)

	path = ""
	cfg = &Configuration{
		Storage: Storage{LogDirectory: path},
	}
	err = cfg.setupDirectory()
	if err == nil {
		t.Error("should have error creating empty dir")
	}
}

