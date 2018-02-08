package main

import (
	"log"
)

// CheckErr - Utility function for checking errors and printing a message relating to what the error is. This cleans up the code and makes things easier to read.
func CheckErr(message string, err error) {
	if err != nil {
		log.Fatalln(message, err)
	}
}
