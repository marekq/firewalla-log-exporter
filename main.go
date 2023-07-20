package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/axiomhq/axiom-go/axiom"
	"github.com/axiomhq/axiom-go/axiom/ingest"

	"github.com/marekq/firewalla-log-exporter/lambda/mystructs"
)

// read axiom config from env variables

var (
	axiomDataset    = ""
	axiomOrgID      = ""
	axiomToken      = ""
	firewallaURL    = ""
	firewallaAPIKey = ""
)

func GetSecret(secretName string, region string) (string, error) {
	ctx := context.Background()

	// Load the SDK's configuration data.
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return "", fmt.Errorf("unable to load SDK config, %v", err)
	}

	client := secretsmanager.NewFromConfig(cfg)

	input := &secretsmanager.GetSecretValueInput{
		SecretId: &secretName,
	}

	result, err := client.GetSecretValue(ctx, input)
	if err != nil {
		return "", fmt.Errorf("unable to retrieve secret value, %v", err)
	}

	secretString := *result.SecretString
	var secretData map[string]interface{}

	err2 := json.Unmarshal([]byte(secretString), &secretData)
	if err2 != nil {
		log.Println(err)
		return "", err
	}

	for k, v := range secretData {
		fmt.Println(k, v)
		if k == "AXIOM_DATASET" {
			axiomDataset = v.(string)
		} else if k == "AXIOM_ORG_ID" {
			axiomOrgID = v.(string)
		} else if k == "AXIOM_TOKEN" {
			axiomToken = v.(string)
		} else if k == "FIREWALLA_URL" {
			firewallaURL = v.(string)
		} else if k == "FIREWALLA_KEY" {
			firewallaAPIKey = v.(string)
		}

	}

	fmt.Println("AXIOM_DATASET:", axiomDataset, "AXIOM_ORG_ID:", axiomOrgID, "AXIOM_TOKEN:", axiomToken, "FIREWALLA_URL:", firewallaURL, "FIREWALLA_KEY:", firewallaAPIKey)

	return *result.SecretString, nil
}

// get flowlogs axiom
func getAxiomFlowlogs(ctx context.Context) {

	fmt.Println("* axiom started with dataset:", axiomDataset, " orgID:", axiomOrgID, " token:", axiomToken)

	// create axiom client
	axiomClient, err := axiom.NewClient(
		axiom.SetPersonalTokenConfig(axiomToken, axiomOrgID),
	)

	if err != nil {
		log.Fatal("Error creating axiom client ", err)
	}

	// create firewalla client
	firewallaClient := &http.Client{
		Timeout: time.Second * 10,
	}

	// create firstTs and lastTs
	firstTs := getAxiomFirstTimestamp(ctx, axiomClient, axiomDataset).Unix()
	lastTs := time.Now().Unix()

	fmt.Println("* axiom start " + time.Unix(firstTs, 0).Format("2006-01-02 15:04:05"))
	fmt.Println("* axiom end   " + time.Unix(lastTs, 0).Format("2006-01-02 15:04:05"))

	// Loop through all hours (i.e. 12:00 - 13:00, 13:00 - 14:00, etc)
	for currentTs := firstTs; currentTs < lastTs; currentTs += 3600 {

		// start and end time of the full hour
		startTime := time.Unix(currentTs, 0)
		endTime := time.Unix(currentTs+3600, 0)

		// get flowlog details
		getAxiomFlowlogDetails(ctx, firewallaClient, axiomClient, startTime, endTime)

	}

}

// get flowlogs axiom first timestamp
func getAxiomFirstTimestamp(ctx context.Context, axiomClient *axiom.Client, axiomDataset string) time.Time {

	// get first timestamp from axiom
	apl := "['" + axiomDataset + "'] | limit 1 | sort by _time desc"
	fmt.Println("* axiom query:", apl)
	res, err := axiomClient.Query(ctx, apl)

	if err != nil {
		log.Fatal("Error querying axiom ", err)
	}

	hours := 1
	ts := time.Now().Add(-time.Duration(hours) * time.Hour)

	fmt.Println(res)

	return ts

}

// make post request to api, return body
func makePostRequest(url string, token string, startTs float64, endTs float64, client *http.Client) []byte {

	postData := map[string]float64{
		"start": startTs,
		"end":   endTs,
	}

	postBody, err := json.MarshalIndent(postData, "", "  ")
	if err != nil {
		log.Fatal("Error marshalling json ", err, postData)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(postBody))
	if err != nil {
		log.Fatal("Error making Firewalla request ", err, url, req)
	}

	// set headers
	req.Header.Set("Authorization", "Token "+token)
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("Error making Firewalla request ", err, url, req)
	}

	if resp.StatusCode != 200 {
		log.Fatal("Firewalla API - Status code error:", resp.StatusCode, resp.Status, resp.Header, url)
	}

	defer resp.Body.Close()

	// convert to []byte
	respBody, _ := io.ReadAll(resp.Body)

	return respBody
}

// get flowlog details axiom
func getAxiomFlowlogDetails(ctx context.Context, firewallaClient *http.Client, axiomClient *axiom.Client, startTime time.Time, endTime time.Time) {

	flowLogCounter := 0

	for {

		startTs := startTime.Unix()
		endTs := endTime.Unix()

		// get flow logs for specific hour (startTs to endTs)
		body := makePostRequest(
			firewallaURL+"flows/query",
			firewallaAPIKey,
			float64(startTs),
			float64(endTs),
			firewallaClient,
		)

		// convert to struct
		flowlogs := []axiom.Event{}
		json.Unmarshal([]byte(body), &flowlogs)

		// create new minTs as float64
		minTs := float64(endTs)

		// loop through flowlog responses line by line
		for _, flowlog := range flowlogs {

			output := convertToAxiomEvent(flowlog)
			flowlogs = append(flowlogs, output)

		}

		fmt.Println("* axiom ingested ", flowLogCounter, " flowlogs, response:", len(flowlogs))

		//Get io.Reader from string
		_, err := axiomClient.IngestEvents(ctx, axiomDataset, []axiom.Event{
			{ingest.TimestampField: time.Now(), "timest": 123},
		})

		if err != nil {
			log.Fatal("Error ingesting events ", err)
		}

		// minTs to string
		minTsString := strconv.FormatFloat(minTs, 'f', 0, 64)

		fmt.Println("* axiom ingested ", flowLogCounter, " flowlogs, timestamp:", minTsString)

	}

}

func convertToAxiomEvent(f mystructs.Flowlog) (output axiom.Event) {

	event := &axiom.Event{}
	event.Source = "firewalla"
	event.EventType = "flowlog"
	event.Timestamp = f.Ts
	event.Tags = f.Tags
	event.Category = f.Category
	event.App = f.App
	event.Device = f.Device
	event.DeviceName = f.DeviceName
	event.DeviceIP = f.DeviceIP
	event.DevicePort = f.DevicePort
	event.DevicePortInfo = f.DevicePortInfo
	event.IP = f.IP
	event.Intf = f.Intf
	event.IntfInfo = f.IntfInfo
	event.Ltype = f.Ltype
	event.MacVendor = f.MacVendor
	event.NetworkName = f.NetworkName
	event.OnWan = f.OnWan
	event.Port = f.Port
	event.PortInfo = f.PortInfo
	event.Protocol = f.Protocol
	event.TagIds = f.TagIds
	event.Total = f.Total
	event.Type = f.Type
	event.Upload = f.Upload
	event.WanIntf = f.WanIntf
	event.Blocked = f.Blocked
	event.BlockPid = f.BlockPid
	event.BlockType = f.BlockType
	event.Gid = f.Gid
	event.GidExtracted = f.GidExtracted
	event.Pid = f.Pid
	event.Duration = f.Duration
	event.Count = f.Count
	event.Download = f.Download
	event.Host = f.Host
	event.Country = f.Country
	event.Fd = f.Fd

	return event

}

func handler(ctx context.Context) {

	GetSecret(os.Getenv("AWS_SECRET_NAME"), os.Getenv("AWS_REGION"))

	// retrieve flowlogs from firewalla
	getAxiomFlowlogs(ctx)

}

func main() {
	lambda.Start(handler)
}
