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
		writeRequest := types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{
						Value: snapshot.MCMXFFNumbers[0],
					},
					"sk": &types.AttributeValueMemberS{
						Value: "type_" + snapshot.EntityType + "#" + "status_" + snapshot.OperatingStatus + "#" + "units_" + strconv.Itoa(snapshot.PowerUnits) + "#" + "drivers_" + strconv.Itoa(snapshot.Drivers),
					},
					"dotNumber": &types.AttributeValueMemberS{
						Value: snapshot.DOTNumber,
					},
					"dbaName": &types.AttributeValueMemberS{
						Value: snapshot.DBAName,
					},
					"address": &types.AttributeValueMemberS{
						Value: snapshot.PhysicalAddress,
					},
					"entityType": &types.AttributeValueMemberS{
						Value: snapshot.EntityType,
					},
					"operationStatus": &types.AttributeValueMemberS{
						Value: snapshot.OperatingStatus,
					},
					"legalName": &types.AttributeValueMemberS{
						Value: snapshot.LegalName,
					},
					"mailingAddress": &types.AttributeValueMemberS{
						Value: snapshot.MailingAddress,
					},
					"phone": &types.AttributeValueMemberS{
						Value: snapshot.Phone,
					},
					"mcs150Year": &types.AttributeValueMemberS{
						Value: snapshot.MCS150Year,
					},
					"drivers": &types.AttributeValueMemberS{
						Value: strconv.Itoa(snapshot.Drivers),
					},
					"powerUnits": &types.AttributeValueMemberS{
						Value: strconv.Itoa(snapshot.PowerUnits),
					},
					"stateCarrierId": &types.AttributeValueMemberS{
						Value: snapshot.StateCarrierID,
					},
					"mcmxNumbers": &types.AttributeValueMemberSS{
						Value: snapshot.MCMXFFNumbers,
					},
					"latestUpdate": &types.AttributeValueMemberS{
						Value: snapshot.LatestUpdateDate.Format(time.DateOnly),
					},
					"cargoCarried": &types.AttributeValueMemberSS{
						Value: snapshot.CargoCarried,
					},
				},
			},
		}

		writeRequests = append(writeRequests, writeRequest)
	}

	return writeRequests
}
