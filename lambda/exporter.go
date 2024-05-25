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

	lambdadetector "go.opentelemetry.io/contrib/detectors/aws/lambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// read axiom config from env variables

var (
	axiomDataset    = os.Getenv("AXIOM_DATASET")
	axiomOrgID      = os.Getenv("AXIOM_ORG_ID")
	axiomToken      = os.Getenv("AXIOM_TOKEN")
	firewallaURL    = os.Getenv("FIREWALLA_URL")
	firewallaAPIKey = os.Getenv("FIREWALLA_KEY")
	timeZone, _     = time.LoadLocation("Europe/Amsterdam")
	timeFormat      = "2006-01-02 15:04:05"
)

// get flowlogs axiom
func getAxiomFlowlogs(ctx context.Context, hoursRetrieve int) {

	fmt.Println("* getAxiomFlowlogs - started with dataset:", axiomDataset, " orgID:", axiomOrgID, " token:", axiomToken)

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
		Transport: otelhttp.NewTransport(
			http.DefaultTransport,
			otelhttp.WithTracerProvider(otel.GetTracerProvider()),
		),
	}

	// create firstTs and lastTs
	firstTs := getAxiomFirstTimestamp(ctx, axiomClient, axiomDataset, hoursRetrieve).Unix()
	lastTs := time.Now().Unix()

	completedFlowlogs := 0

	// start and end time of the full hour
	startTime := time.Unix(firstTs, 0)
	endTime := time.Unix(lastTs, 0)

	// get flowlog details
	getAxiomFlowlogDetails(ctx, *firewallaClient, axiomClient, startTime, endTime, completedFlowlogs)

}

// get flowlogs axiom first timestamp
func getAxiomFirstTimestamp(ctx context.Context, axiomClient *axiom.Client, axiomDataset string, hoursRetrieve int) time.Time {

	now := float64(time.Now().Unix())
	firstTs := now - float64(hoursRetrieve*3600)

	// get first timestamp from axiom
	apl := axiomDataset + " | order by _time desc | limit 1"
	res, err := axiomClient.Query(ctx, apl)

	if err != nil {
		log.Fatal("- Error querying axiom ", err, apl)

	} else if len(res.Matches) == 0 {
		fmt.Println("- No matches found in axiom query ", apl)
	}

	// get first timestamp from result
	if len(res.Matches) > 0 {

		foundTsStr := res.Matches[0].Data["ingest_timestamp"]

		t, err := time.Parse(timeFormat, foundTsStr.(string))
		if err != nil {
			log.Fatal("- Error parsing timestamp ", err, foundTsStr)
		}
		foundTs := float64(t.Unix())

		if float64(foundTs) > firstTs {
			firstTs = foundTs
		}
	}

	fmt.Println("* getAxiomFirstTimestamp - got last timestamp from axiom ", time.Unix(int64(firstTs), 0).In(timeZone).Format(timeFormat))
	fmt.Printf("* getAxiomFirstTimestamp - hours difference from now %.2f\n", (now-firstTs)/3600)

	return time.Unix(int64(firstTs), 0)

}

// make post request to api, return body
func makeGetRequest(url string, client http.Client) Response {

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
func getAxiomFlowlogDetails(ctx context.Context, firewallaClient http.Client, axiomClient *axiom.Client, startTime time.Time, endTime time.Time, completedFlowlogs int) error {

	startTs := startTime.Unix()
	endTs := endTime.Unix()
	minTs := int64(startTs)

	for {

		furl := firewallaURL + "flows?begin=" + strconv.FormatInt(startTs, 10) + "&end=" + strconv.FormatInt(endTs, 10) + "&limit=500"

		// print human readable time for start and end
		fmt.Println("* getAxiomFlowlogDetails - start " + startTime.In(timeZone).Format(timeFormat) + " end " + endTime.In(timeZone).Format(timeFormat))

		// get flow logs for specific hour (startTs to endTs)
		body := makeGetRequest(
			furl,
			firewallaClient,
		)

		// create new minTs as float64
		postEvent := []axiom.Event{}

		for _, flowlog := range body.Results {

			nowTs := time.Now().Unix()

			// if not empty
			if flowlog.Device.IP != "" {

				completedFlowlogs++

				// add to postEvent
				postEvent = append(postEvent, axiom.Event{
					ingest.TimestampField: time.Unix(int64(flowlog.Ts), 0).Format(timeFormat),
					"event_timestamp":     time.Unix(int64(flowlog.Ts), 0).Format(timeFormat),
					"ingest_timestamp":    time.Unix(int64(nowTs), 0).Format(timeFormat),
					"ip":                  flowlog.Device.IP,
					"device_name":         flowlog.Device.Name,
					"device_port":         flowlog.Device.Port,
					"device_id":           flowlog.Device.Id,
					"device_network_id":   flowlog.Device.Network.Id,
					"device_network_name": flowlog.Device.Network.Name,
					"device_group_id":     flowlog.Device.Group.Id,
					"device_group_name":   flowlog.Device.Group.Name,
					"remote_ip":           flowlog.Remote.Ip,
					"remote_domain":       flowlog.Remote.Domain,
					"remote_port":         flowlog.Remote.Port,
					"remote_country":      flowlog.Remote.Country,
					"gid":                 flowlog.Gid,
					"protocol":            flowlog.Protocol,
					"direction":           flowlog.Direction,
					"block":               flowlog.Block,
					"count":               flowlog.Count,
					"download":            flowlog.Download,
					"upload":              flowlog.Upload,
					"duration":            flowlog.Duration,
				})

				// if flowlog ts is less than minTs, set minTs to flowlog ts
				if flowlog.Ts > float64(minTs) {
					minTs = int64(flowlog.Ts)
					startTs = minTs

				}
			}
		}

		// ingest axiom events
		_, err := axiomClient.IngestEvents(
			ctx,
			axiomDataset,
			postEvent,
		)

		if err != nil {
			log.Fatal("- Error ingesting events ", err)
		}

		fmt.Println("* getAxiomFlowlogDetails - ingested " + strconv.Itoa(len(postEvent)) + " flowlogs")

		// if record count is less than 1000, break
		if body.Count < 1000 || body.Next == 0 {

			break
		}
	}

	fmt.Println("* getAxiomFlowlogDetails - done with " + strconv.Itoa(completedFlowlogs) + " flowlogs")

	return nil

}

type Event struct {
	HoursRetrieve int `json:"hoursRetrieve"`
}

func Handler(ctx context.Context) {

	// get for 12 hours
	getAxiomFlowlogs(ctx, 12)

}

func main() {

	ctx := context.Background()

	detector := lambdadetector.NewResourceDetector()
	res, err := detector.Detect(ctx)
	if err != nil {
		log.Fatalf("failed to detect lambda resources: %v\n", err)
		return
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	lambda.Start(otellambda.InstrumentHandler(Handler, otellambda.WithTracerProvider(tp)))

}

//////////////////////////

type Response struct {
	Results []struct {
		Ts        float64 `json:"ts"`
		Gid       string  `json:"gid"`
		Protocol  string  `json:"protocol"`
		Direction string  `json:"direction"`
		Block     bool    `json:"block"`
		Count     int     `json:"count"`
		Download  int     `json:"download"`
		Upload    int     `json:"upload"`
		Duration  float64 `json:"duration"`
		Device    struct {
			IP      string `json:"ip"`
			Name    string `json:"name"`
			Port    int    `json:"port"`
			Id      string `json:"id"`
			Network struct {
				Id   string `json:"id"`
				Name string `json:"name"`
			} `json:"network"`
			Group struct {
				Id   string `json:"id"`
				Name string `json:"name"`
			} `json:"group"`
		} `json:"device"`
		Remote struct {
			Ip      string `json:"ip"`
			Domain  string `json:"domain"`
			Port    int    `json:"port"`
			Country string `json:"country"`
		} `json:"remote"`
	} `json:"results"`
	Count int     `json:"count"`
	Next  float64 `json:"next"`
}
