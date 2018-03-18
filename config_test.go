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
	f1, _ := os.Create(f)
	f1.Write([]byte("bad toml file"))
	f1.Close()
	_, err = NewConfiguration(f)
	if err == nil {
		t.Errorf("should have error reading bad toml file")
	}
	// clean up test file
	os.Remove(f)
}

func TestGetApp(t *testing.T) {
	// build a config with 1 App
	a1 := App{Name: "App 1"}
	cfg := &Configuration{
		Apps: []App{a1},
	}
	_, err := cfg.getApp("App That Doesn't Exist")
	if err == nil {
		t.Errorf("Err not returned when app doesn't exist")
	}

	a1Retrieved, _ := cfg.getApp("App 1")
	if !cmp.Equal(a1, a1Retrieved) {
		t.Errorf("a1 was not equal")
	}
}

func TestCompileRegex(t *testing.T) {
	a1 := App{
		Name:  "App 1",
		Regex: `([\d.]+) - \[([^\]]*)\] - - \[([^\]]*)\]`,
	}
	cfg := &Configuration{
		Apps: []App{a1},
	}
	if cfg.Apps[0].CompiledRegex != nil {
		t.Errorf("compile regex is not nil but compile hasn't been called yet")
	}
	cfg.compileRegex()
	if cfg.Apps[0].CompiledRegex == nil {
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

func TestExtractAppNames(t *testing.T) {
	cfg := &Configuration{
		Apps: []App{{Name: "a1"}, {Name: "a2"}},
	}
	names := cfg.extractAppNames()
	expected := make(map[string]struct{}, len(cfg.Apps))
	expected["a1"] = struct{}{}
	expected["a2"] = struct{}{}
	if !cmp.Equal(names, expected) {
		t.Errorf("expected names to equal %v, received %v instead", expected, names)
	}
}
