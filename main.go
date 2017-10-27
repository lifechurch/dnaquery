package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/buger/jsonparser"
)

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

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s infile outfile\n", os.Args[0])
		return
	}
	inFile := os.Args[1]
	outFile := os.Args[2]
	ch := readLine(inFile)
	processLine(outFile, ch)
}
