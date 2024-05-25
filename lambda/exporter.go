package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
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

	fmt.Println("* getAxiomFlowlogs - started with dataset:", axiomDataset, " orgID:", axiomOrgID, " token:", axiomToken, " hoursRetrieve:", hoursRetrieve)

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

	// start and end time of the full hour
	startTime := time.Unix(firstTs, 0)
	endTime := time.Unix(lastTs, 0)

	// get flowlog details
	countRetrieved := getAxiomFlowlogDetails(ctx, *firewallaClient, axiomClient, startTime, endTime)

	fmt.Println("* getAxiomFlowlogs - completed with dataset:", axiomDataset, " orgID:", axiomOrgID, " token:", axiomToken, " retrieved:", countRetrieved, " flowlogs")

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
func getAxiomFlowlogDetails(ctx context.Context, firewallaClient http.Client, axiomClient *axiom.Client, startTime time.Time, endTime time.Time) int {

	completedFlowlogs := 0
	startTs := startTime.Unix()
	endTs := endTime.Unix()
	minTs := int64(startTs)

	cursor := ""
	groupBy := "ts,status,box,source,sourceIP,sport,device,network,destination,destinationIP,dport,domain,protocol,category,region,direction,blockType,upload,download,total,count"
	encodedGroupBy := "groupBy=" + url.QueryEscape(groupBy)

	// print human readable time for start and end
	fmt.Println("start " + startTime.In(timeZone).Format(timeFormat) + " \nend   " + endTime.In(timeZone).Format(timeFormat) + " \ndiff  " + strconv.Itoa(int(endTime.Sub(startTime).Seconds())) + " seconds")

	for {

		furl := firewallaURL + "flows?query=ts%3A" + strconv.FormatInt(startTs, 10) + "-" + strconv.FormatInt(endTs, 10) + "&sortBy=ts&limit=500&" + encodedGroupBy + "&cursor=" + url.QueryEscape(cursor)

		// get flow logs for specific hour (startTs to endTs)
		body := makeGetRequest(
			furl,
			firewallaClient,
		)

		// create new minTs as float64
		postEvent := []axiom.Event{}
		cursor = body.NextCursor

		for _, flowlog := range body.Results {

			nowTs := time.Now().Unix()

			// if not empty
			if flowlog.Device.IP != "" {

				completedFlowlogs++

				// add to postEvent
				postEvent = append(postEvent, axiom.Event{
					ingest.TimestampField: time.Unix(int64(flowlog.TS), 0).Format(timeFormat),
					"event_timestamp":     time.Unix(int64(flowlog.TS), 0).Format(timeFormat),
					"ingest_timestamp":    time.Unix(int64(nowTs), 0).Format(timeFormat),
					"gid":                 flowlog.GID,
					"protocol":            flowlog.Protocol,
					"direction":           flowlog.Direction,
					"block":               flowlog.Block,
					"blockType":           flowlog.BlockType,
					"download":            flowlog.Download,
					"upload":              flowlog.Upload,
					"duration":            flowlog.Duration,
					"count":               flowlog.Count,
					"device": map[string]interface{}{
						"id":   flowlog.Device.ID,
						"ip":   flowlog.Device.IP,
						"name": flowlog.Device.Name,
					},
					"source": map[string]interface{}{
						"id":   flowlog.Source.ID,
						"name": flowlog.Source.Name,
						"ip":   flowlog.Source.IP,
					},
					"destination": map[string]interface{}{
						"id":   flowlog.Destination.ID,
						"name": flowlog.Destination.Name,
						"ip":   flowlog.Destination.IP,
					},
					"region":   flowlog.Region,
					"category": flowlog.Category,
					"network": map[string]interface{}{
						"id":   flowlog.Network.ID,
						"name": flowlog.Network.Name,
					},
				})

				// if flowlog ts is less than minTs, set minTs to flowlog ts
				if flowlog.TS > float64(minTs) {
					minTs = int64(flowlog.TS)
					startTs = minTs

				}
			}
		}

		// ingest axiom events
		fmt.Println("* axiomClient.IngestEvents - count ", len(postEvent))
		_, err := axiomClient.IngestEvents(
			ctx,
			axiomDataset,
			postEvent,
		)

		if err != nil {
			log.Fatal("- Error ingesting events ", err)
		}

		// if record count is less than 500, break
		if body.Count < 500 || body.NextCursor == "" {

			break
		}
	}

	return completedFlowlogs

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
		TS        float64   `json:"ts"`
		GID       string  `json:"gid"`
		Protocol  string  `json:"protocol"`
		Direction string  `json:"direction"`
		Block     bool    `json:"block"`
		BlockType *string `json:"blockType,omitempty"`
		Download  *int64  `json:"download,omitempty"`
		Upload    *int64  `json:"upload,omitempty"`
		Duration  *int64  `json:"duration,omitempty"`
		Count     int     `json:"count"`
		Device    struct {
			ID   string `json:"id"`
			IP   string `json:"ip"`
			Name string `json:"name"`
		} `json:"device"`
		Source struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			IP   string `json:"ip"`
		} `json:"source,omitempty"`
		Destination struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			IP   string `json:"ip"`
		} `json:"destination,omitempty"`
		Region   *string `json:"region,omitempty"`
		Category *string `json:"category,omitempty"`
		Network  struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"network"`
	} `json:"results"`
	Count      int    `json:"count"`
	NextCursor string `json:"next_cursor"`
}
