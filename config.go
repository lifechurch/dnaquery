package main

import (
	"fmt"
	"github.com/pelletier/go-toml"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"regexp"
)

// Storage holds the configuration for [storage] section of the toml config.
type Storage struct {
	LogDirectory string
}

// GCP holds the configuration for [gcp] section of the toml config.
type GCP struct {
	ProjectID       string
	CredentialsFile string
	UploadBucket    string
	Dataset         string
	TemplateTable   string
	LogPrefix       string
	LogBucket       string
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

// Configuration holds the full configuration loaded from the toml config file.
type Configuration struct {
	Storage    Storage
	GCP        GCP
	Containers []Container
}

func (cfg *Configuration) getContainer(c string) (Container, error) {
	for _, container := range cfg.Containers {
		if c == container.Name {
			return container, nil
		}
	}
	return Container{}, errors.New("Container not found")
}

func (cfg *Configuration) extractContainerNames() (set map[string]struct{}) {
	set = make(map[string]struct{}, len(cfg.Containers))
	for _, container := range cfg.Containers {
		set[container.Name] = struct{}{}
	}
	return set
}

func (cfg *Configuration) compileRegex() {
	for i, c := range cfg.Containers {
		cmp := regexp.MustCompile(c.Regex)
		cfg.Containers[i].CompiledRegex = cmp
	}
}

func (cfg *Configuration) setupDirectory() error {
	err := os.MkdirAll(cfg.Storage.LogDirectory, os.ModePerm)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Unable to create dir %s", cfg.Storage.LogDirectory))
	}
	return err
}

// NewConfiguration takes a path to a toml file and returns a new Configuration
func NewConfiguration(path string) (*Configuration, error) {
	cfg := &Configuration{}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Unable to open config (%s)", path))
	}
	err = toml.Unmarshal(data, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "Error loading config")
	}
	return cfg, nil
}
