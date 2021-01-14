package main

import (
  "context"
  "crypto/tls"
  "encoding/json"
  "fmt"
  "io/ioutil"
  "net/http"
  "time"

  eventhub "github.com/Azure/azure-event-hubs-go"
  "github.com/go-redis/redis"
  "github.com/sirupsen/logrus"
)

// Azure Connection Strings
const (
  eventHubConnStr = "Endpoint=sb://iotlab.servicebus.windows.net/;SharedAccessKeyName=iotlab;SharedAccessKey=2Olj44OhaEu0iX/cZSVR4HdlfTdKWd41FH3u2vxj2vo=;EntityPath=iotlab"
  redisAddr = "iotlab.redis.cache.windows.net:6380"
  redisPass = "6gq+cU7jM7YLlHOBQ0m2+Ui9w9mXujfItOmwv2hM0T4="
)

type postRequest struct {
  Url      string `json:"url"`
  Strategy string `json:"strategy"`
}

// Json File Parsing and splitting to arrays
func getJson(url string) []map[string]string {
  getClient := http.Client{
    Timeout: time.Second * 2, // Timeout after 2 seconds
  }

  req, err := http.NewRequest(http.MethodGet, url, nil)
  if err != nil {
    logrus.Fatal(err)
  }

  res, err := getClient.Do(req)
  if err != nil {
    logrus.Fatal(err)
  }

  if res.Body != nil {
    defer res.Body.Close()
  }

  body, err := ioutil.ReadAll(res.Body)
  if err != nil {
    logrus.Fatal(err)
  }

  var jsonArr []map[string]string

  err = json.Unmarshal(body, &jsonArr)
  if err != nil {
    logrus.Fatal(err)
  }

  return jsonArr
}

// Write Json to Redis
func operateRedis(url string) {
  dataJson := getJson(url)
  client := redis.NewClient(&redis.Options{
    Addr:      redisAddr,
    Password:  redisPass,
    DB:        0,
    TLSConfig: &tls.Config{InsecureSkipVerify: true},
  })

  _, err := client.Ping().Result()
  if err != nil {
    logrus.Fatal(err)
  }

  for i, data := range dataJson {
    dataSingleJson, err := json.Marshal(data)
    if err != nil {
      logrus.Fatal(err)
    }
    err = client.Set(fmt.Sprintf("data_%d", i), dataSingleJson, 0).Err()
    if err != nil {
      logrus.Warn(err)
    } else {
      logrus.Infof("Document %d is written!", i)
    }
  }
}

// Write Json to EventHub
func operateEventHub(url string) {
  // get Json
  dataJson := getJson(url)
  
  hub, err := eventhub.NewHubFromConnectionString(eventHubConnStr)
  if err != nil {
    logrus.Warn(err)
  }

  ctx := context.Background()

  // send a single message into a random partition
  for i, data := range dataJson {
    dataSingleJson, err := json.Marshal(data)
    if err != nil {
      logrus.Fatal(err)
    }
    event := eventhub.NewEvent(dataSingleJson)
    event.Set("content_type", "application/json")
    err = hub.Send(ctx, event)
    if err != nil {
      logrus.Warn(err)
    } else {
      logrus.Infof("Documant %d was sent!", i)
    }
  }

  err = hub.Close(context.Background())
  if err != nil {
    logrus.Info(err)
  }

  logrus.Info("Json was sent to EventHub!")
}


// Strategy Selection
func HelloServer(w http.ResponseWriter, req *http.Request) {
  decoder := json.NewDecoder(req.Body)
  var request postRequest
  err := decoder.Decode(&request)
  if err != nil {
    panic(err)
  }

  fmt.Println(request.Strategy)

  switch {
  case request.Strategy == "redis":
    operateRedis(request.Url)
  case request.Strategy == "eventHub":
    operateEventHub(request.Url)
  default:
    logrus.Info("Wrong strategy choosed!")
  }
}

// Server Start
func main() {
  http.HandleFunc("/url", HelloServer)
  http.ListenAndServe(":9000", nil)
}