package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	ServerID = iota
	ServerIP
	ServerRPCListenerPort
)

func ParseClusterConfig(numOfServers int, path string) (info [][]string) {

	var fileRows []string

	s, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	defer func() {
		err := s.Close()
		if err != nil {
			panic(err)
		}
	}()

	scanner := bufio.NewScanner(s)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		
		fileRows = append(fileRows, line)
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		panic(fmt.Sprintf("Error reading config: %v", err))
	}

	if len(fileRows) < numOfServers {
		err := fmt.Sprintf("insufficient configs for servers | # rows: %v | # servers: %v", len(fileRows), numOfServers)
		panic(errors.New(err))
	}

	for i := 0; i < len(fileRows); i++ {
		row := strings.Fields(fileRows[i])  // Handles multiple spaces
		
		if len(row) < 3 {
			panic(fmt.Sprintf("Config line %d malformed: need 3 fields (ID IP PORT), got %d: %q",
				i+1, len(row), fileRows[i]))
		}
		
		info = append(info, row)
	}

	return info
}
