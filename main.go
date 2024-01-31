package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/brandenc40/safer"
	"github.com/zohaibakber/safer-scraper/operations"
)

func main() {

	client := safer.NewClient()
	var successCount int
	var currentArrayCount int
	var snapshots []safer.CompanySnapshot
	for mcmx := 404; mcmx <= 200000; mcmx++ {
		snapshot, err := client.GetCompanyByMCMX(fmt.Sprintf("%d", mcmx))
		if err != nil {
			fmt.Println("Error for MCMX:", mcmx, "Error:", err.Error())
			randomSleep := time.Duration(rand.Intn(4-1+1)+1) * time.Second
			time.Sleep(randomSleep)
			snapshot = nil
			continue
		}

		snapshots = append(snapshots, *snapshot)

		if len(snapshots) == 25 {
			err = operations.PutItem(&snapshots)
			if err != nil {
				fmt.Println("Error for MCMX:", mcmx, "Error:", err.Error())
				continue
			}
			// Clear the snapshots slice for the next batch
			snapshots = nil
			currentArrayCount = len(snapshots)
		}
		successCount++

		fmt.Println("Success for MCMX:", mcmx, "Total successes:", successCount, "Current array count:", currentArrayCount)
		time.Sleep(8 * time.Second)
	}

	// Put the remaining snapshots that are <25
	if len(snapshots) > 0 {
		err := operations.PutItem(&snapshots)
		if err != nil {
			fmt.Println("Error:", err.Error())
		}
	}

}
