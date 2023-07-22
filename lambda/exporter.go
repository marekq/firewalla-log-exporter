package main

import (
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
	"github.com/axiomhq/axiom-go/axiom"
	"github.com/axiomhq/axiom-go/axiom/ingest"
)

// read axiom config from env variables

var (
	axiomDataset    = os.Getenv("AXIOM_DATASET")
	axiomOrgID      = os.Getenv("AXIOM_ORG_ID")
	axiomToken      = os.Getenv("AXIOM_TOKEN")
	firewallaURL    = os.Getenv("FIREWALLA_URL")
	firewallaAPIKey = os.Getenv("FIREWALLA_KEY")
)

// get flowlogs axiom
func getAxiomFlowlogs(ctx context.Context, hoursRetrieve int) {

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
	firstTs := getAxiomFirstTimestamp(ctx, axiomClient, axiomDataset, hoursRetrieve).Unix()
	lastTs := time.Now().Unix()

	completedFlowlogs := 0

	// start and end time of the full hour
	startTime := time.Unix(firstTs, 0)
	endTime := time.Unix(lastTs, 0)

	// get flowlog details
	getAxiomFlowlogDetails(ctx, firewallaClient, axiomClient, startTime, endTime, completedFlowlogs)

}

// get flowlogs axiom first timestamp
func getAxiomFirstTimestamp(ctx context.Context, axiomClient *axiom.Client, axiomDataset string, hoursRetrieve int) time.Time {

	firstTs := 0

	// get first timestamp from axiom
	apl := axiomDataset + " | limit 1 | sort by _time desc"
	res, err := axiomClient.Query(ctx, apl)

	if err != nil {
		log.Fatal("Error querying axiom ", err)

	} else if len(res.Matches) == 0 {
		log.Fatal("No matches found")
	}

	// get first timestamp from result
	for _, match := range res.Matches {

		// pretty print
		for k, v := range match.Data {

			if k == "ts" {
				firstTs = int(v.(float64))
			}
		}

	}

	now := float64(time.Now().Unix())

	// if firstTs is more than 12 hours ago, set to 12 hours ago
	if now-float64(firstTs) > float64(hoursRetrieve*3600) {
		firstTs = int(now - float64(hoursRetrieve*3600))
	}

	fmt.Println("* starting from", time.Unix(int64(firstTs), 0))
	return time.Unix(int64(firstTs), 0)

}

// make post request to api, return body
func makeGetRequest(url string, client *http.Client) Response {

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal("Error creating Firewalla request ", err, url, req)
	}

	// set headers
	req.Header.Add("Authorization", "Token "+firewallaAPIKey)

	resp, err2 := client.Do(req)
	if err2 != nil {
		log.Fatal("Error making Firewalla request ", resp.StatusCode, err, url, req)
	}

	if resp.StatusCode != 200 {
		log.Fatal("Non 200 response ", resp.StatusCode, url, req)
	}

	respBody, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()

	var response Response
	json.Unmarshal(respBody, &response)

	resp.Body.Close()

	return response
}

// get flowlog details axiom
func getAxiomFlowlogDetails(ctx context.Context, firewallaClient *http.Client, axiomClient *axiom.Client, startTime time.Time, endTime time.Time, completedFlowlogs int) error {

	startTs := startTime.Unix()
	endTs := endTime.Unix()
	minTs := int64(startTs)

	for {

		furl := firewallaURL + "flows?begin=" + strconv.FormatInt(startTs, 10) + "&end=" + strconv.FormatInt(endTs, 10) + "&limit=1000"

		// print human readable time for start and end
		fmt.Println("* firewalla start " + startTime.Format("2006-01-02 15:04:05") + " end " + endTime.Format("2006-01-02 15:04:05"))

		// get flow logs for specific hour (startTs to endTs)
		body := makeGetRequest(
			furl,
			firewallaClient,
		)

		// create new minTs as float64
		postEvent := []axiom.Event{}

		for _, flowlog := range body.Results {

			completedFlowlogs++

			nowTs := time.Now().Unix()

			// add to postEvent
			postEvent = append(postEvent, axiom.Event{
				ingest.TimestampField: flowlog.Ts,
				"event_timestamp":     int(flowlog.Ts),
				"ingest_timestamp":    int(nowTs),
				"ip":                  flowlog.Device.IP,
				"device_name":         flowlog.Device.Name,
				"device_port":         flowlog.Device.Port,
				"device_id":           flowlog.Device.ID,
				"device_network_id":   flowlog.Device.Network.ID,
				"device_network_name": flowlog.Device.Network.Name,
				"device_group_id":     flowlog.Device.Group.ID,
				"device_group_name":   flowlog.Device.Group.Name,
				"remote_ip":           flowlog.Remote.IP,
				"remote_domain":       flowlog.Remote.Domain,
				"remote_port":         flowlog.Remote.Port,
				"remote_country":      flowlog.Remote.Country,
				"gid":                 flowlog.Gid,
				"protocol":            flowlog.Protocol,
				"direction":           flowlog.Direction,
				"block":               flowlog.Block,
				"count":               int(flowlog.Count),
				"download":            flowlog.Download,
				"upload":              flowlog.Upload,
				"duration":            flowlog.Duration,
			})

			// if flowlog ts is less than minTs, set minTs to flowlog ts
			if flowlog.Ts > minTs {
				minTs = flowlog.Ts
				startTs = minTs

			}
		}

		_, err := axiomClient.IngestEvents(ctx, axiomDataset, postEvent)
		if err != nil {
			log.Fatal("Error ingesting events ", err)
		}

		fmt.Println("ingested " + strconv.Itoa(len(postEvent)) + " flowlogs, completed " + strconv.Itoa(completedFlowlogs) + " flowlogs, minTs " + strconv.FormatInt(int64(minTs), 10))

		// get record count
		recordCount := len(body.Results)

		// if record count is less than 1000, break
		if recordCount < 1000 || body.Next == 0 {

			break
		}
	}

	fmt.Println("Done with " + strconv.Itoa(completedFlowlogs) + " flowlogs")
	return nil

}

type Event struct {
	HoursRetrieve int `json:"hoursRetrieve"`
}

func processEvent(event json.RawMessage) (Event, error) {
	var e Event

	err := json.Unmarshal(event, &e)
	if err != nil {
		fmt.Println(err)
		return e, err
	}

	if e.HoursRetrieve == 0 {
		e.HoursRetrieve = 1
	}

	return e, nil
}

func handler(ctx context.Context, event json.RawMessage) {

	e, err := processEvent(event)
	if err != nil {
		fmt.Println("Error processing input event ", err)
	}

	fmt.Println("Retrieving flowlogs from Firewalla for the last " + strconv.Itoa(e.HoursRetrieve) + " hours")
	getAxiomFlowlogs(ctx, e.HoursRetrieve)

}

func main() {
	lambda.Start(handler)
}

//////////////////////////

type Response struct {
	Results []Result `json:"results"`
	Count   int      `json:"count"`
	Next    float64  `json:"next"`
}

type Result struct {
	Ts        int64   `json:"ts"`
	Gid       string  `json:"gid"`
	Protocol  string  `json:"protocol"`
	Direction string  `json:"direction"`
	Block     bool    `json:"block"`
	Count     int     `json:"count"`
	Download  int     `json:"download"`
	Upload    int     `json:"upload"`
	Duration  float64 `json:"duration"`
	Device    Device  `json:"device"`
	Remote    Remote  `json:"remote"`
}

type Device struct {
	IP      string  `json:"ip"`
	Name    string  `json:"name"`
	Port    int     `json:"port"`
	ID      string  `json:"id"`
	Network Network `json:"network"`
	Group   Group   `json:"group"`
}

type Network struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Group struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Remote struct {
	IP      string `json:"ip"`
	Domain  string `json:"domain"`
	Port    int    `json:"port"`
	Country string `json:"country"`
}
