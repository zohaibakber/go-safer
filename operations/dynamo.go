package operations

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/brandenc40/safer"
)

func PutItem(sn *[]safer.CompanySnapshot) error {
	cfg, err := config.LoadDefaultConfig(context.TODO(), func(opts *config.LoadOptions) error {
		opts.Region = "us-east-1"
		return nil
	})
	if err != nil {
		panic(err)
	}

	svc := dynamodb.NewFromConfig(cfg)
	_, err = svc.BatchWriteItem(context.TODO(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"madmax-nub-Table": generateWriteRequests(sn),
		},
	})
	fmt.Println("Wrote", len(*sn), "items to DynamoDB")
	if err != nil {
		fmt.Println("Error:", err.Error())
	}
	return err
}

func generateWriteRequests(snapshots *[]safer.CompanySnapshot) []types.WriteRequest {
	var writeRequests []types.WriteRequest

	for _, snapshot := range *snapshots {
		item := map[string]types.AttributeValue{}

		fields := map[string]string{
			"pk":              snapshot.MCMXFFNumbers[0],
			"sk":              "type_" + snapshot.EntityType + "#" + "status_" + snapshot.OperatingStatus + "#" + "units_" + strconv.Itoa(snapshot.PowerUnits) + "#" + "drivers_" + strconv.Itoa(snapshot.Drivers),
			"dotNumber":       snapshot.DOTNumber,
			"dbaName":         snapshot.DBAName,
			"address":         snapshot.PhysicalAddress,
			"entityType":      snapshot.EntityType,
			"operationStatus": snapshot.OperatingStatus,
			"legalName":       snapshot.LegalName,
			"mailingAddress":  snapshot.MailingAddress,
			"phone":           snapshot.Phone,
			"mcs150Year":      snapshot.MCS150Year,
			"drivers":         strconv.Itoa(snapshot.Drivers),
			"powerUnits":      strconv.Itoa(snapshot.PowerUnits),
			"stateCarrierId":  snapshot.StateCarrierID,
			"latestUpdate":    snapshot.LatestUpdateDate.Format(time.DateOnly),
		}

		for fieldName, fieldValue := range fields {
			if fieldValue != "" {
				item[fieldName] = &types.AttributeValueMemberS{
					Value: fieldValue,
				}
			}
		}
		if len(snapshot.CargoCarried) > 0 {
			item["cargoCarried"] = &types.AttributeValueMemberSS{Value: snapshot.CargoCarried}
		}
		if len(snapshot.MCMXFFNumbers) > 0 {
			mcmxNumberSet := make(map[string]bool)
			for _, number := range snapshot.MCMXFFNumbers {
				mcmxNumberSet[number] = true
			}

			uniqueMCMXNumbers := make([]string, 0, len(mcmxNumberSet))
			for number := range mcmxNumberSet {
				uniqueMCMXNumbers = append(uniqueMCMXNumbers, number)
			}

			item["mcmxNumbers"] = &types.AttributeValueMemberSS{Value: uniqueMCMXNumbers}
		}
		if len(item) > 0 {
			writeRequest := types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: item,
				},
			}
			writeRequests = append(writeRequests, writeRequest)
		}
	}

	return writeRequests
}
