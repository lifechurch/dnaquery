package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/buger/jsonparser"
)

// {"container":"kube-lego","msg":"ignoring as has no annotiation 'kubernetes.io/tls-acme'","_logtype":"json","pod":"kube-lego-3323932148-6w5hh","_lid":"863840205323907074","level":"info","_ip":"172.16.123.31","_ipremote":"169.53.10.231","_host":"kube-lego-3323932148-6w5hh","_app":"kube-lego","_file":"/var/log/containers/kube-lego-3323932148-6w5hh_kube-lego_kube-lego-01e71040cfe46a31586c3e23e9b3602fa3bf5ea3a1a81828c1a416d27d975203.log","node":"worker01","namespace":"bible","context":"ingress","name":"review-upgrade-no-6vqq6m","time":"2017-09-21T00:59:13Z","_mac":"ca:dc:70:a3:e0:ef","containerid":"01e71040cfe46a31586c3e23e9b3602fa3bf5ea3a1a81828c1a416d27d975203","_line":"time=\"2017-09-21T00:59:13Z\" level=info msg=\"ignoring as has no annotiation 'kubernetes.io/tls-acme'\" context=ingress name=review-upgrade-no-6vqq6m namespace=bible ","_ts":1505955553626}
func readLine(path string, ch chan string, stats chan int) {
	inFile, _ := os.Open(path)
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	count := 0
	for scanner.Scan() {
		data := scanner.Bytes()
		container, _ := jsonparser.GetString(data, "container")
		if container != "plans-app-production" {
			continue
		}
		line, _ := jsonparser.GetString(data, "_line")
		ch <- line
		v := 100000
		if (count % v) == 0 {
			stats <- v
		}
		count++
	}
	close(ch)
	close(stats)
}

func parseLine(ch chan string) {
	for l := range ch {
		strings.Split(l, " ")
	}
}

func main() {
	ch := make(chan string, 50)
	go parseLine(ch)
	stats := make(chan int, 10)
	go readLine("85c19b567a.2017-09-21.json", ch, stats)
	for i := range stats {
		fmt.Printf("%d lines processed\n", i)
	}
	fmt.Println("Done")
}
