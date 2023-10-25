package main

import (
	"encoding/csv"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/xrash/smetrics"
)

type QueryData struct {
	Name           string  `json:"name"`
	MinScore       float64 `json:"min_score"`
	DOB            string  `json:"dob,omitempty"`
	DOBMonthsRange int     `json:"dob_months_range,omitempty"`
}

type Result struct {
	TotalHits int      `json:"total_hits"`
	Hits      []Entity `json:"hits"`
}

type Entity struct {
	Name string `json:"name"`
}

var df [][]string

func fuzzySearch(name string, minSimilarity float64) []string {
	var results []string

	// Check for exact matches first
	for _, record := range df {
		if record[1] == name {
			return []string{name}
		}
	}

	// Conduct fuzzy search
	for _, record := range df {
		if len(record) > 1 { // Make sure there's a name to compare against
			similarity := smetrics.JaroWinkler(name, record[1], 0.7, 4)
			if similarity >= minSimilarity {
				results = append(results, record[1])
			}
		}
	}
	return results
}

func performSearch(queryData QueryData) Result {
	name := queryData.Name

	matchingNames := fuzzySearch(name, queryData.MinScore)

	results := Result{
		TotalHits: len(matchingNames),
		Hits:      []Entity{},
	}

	for _, match := range matchingNames {
		results.Hits = append(results.Hits, Entity{Name: match})
	}

	return results
}

// Load the data
func loadData(filePath string) [][]string {
	var data [][]string
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open CSV: %s", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	lineNumber := 1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV at line %d: %s", lineNumber, err)
			lineNumber++
			continue
		}
		data = append(data, record)
		lineNumber++
	}
	return data
}

func main() {
	gin.SetMode(gin.ReleaseMode) // Switch to "release" mode
	r := gin.Default()

	r.POST("/screen_entity", func(c *gin.Context) {
		var requestData struct {
			Query QueryData `json:"query"`
		}
		if err := c.ShouldBindJSON(&requestData); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		matchedResults := performSearch(requestData.Query)
		c.JSON(http.StatusOK, matchedResults)
	})

	df = loadData("sdn.csv")

	r.Run()
}
