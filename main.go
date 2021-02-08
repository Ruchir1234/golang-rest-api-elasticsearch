package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	elastic "gopkg.in/olivere/elastic.v5"
)

type Employee struct {
	ID        string `json:"id,omitempty"`
	Firstname string `json:"first_name,omitempty"`
	Lastname  string `json:"last_name,omitempty"`
	Place     string `json:"place,omitempty"`
	EmailID   string `json:"email_id,omitempty"`
}

var client *elastic.Client

const (
	esIndex = "employee-db"
	esType  = "doc"
)

func AddEmployeeInfo(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("content-type", "application/json")
	var employee Employee
	_ = json.NewDecoder(request.Body).Decode(&employee)
	ctx := context.Background()
	esResp, err := client.Index().Index(esIndex).Type(esType).Id(employee.ID).BodyJson(employee).Do(ctx)
	if err != nil {
		log.Fatal(err)
	}
	docResp := make(map[string]interface{})
	dataBytes, _ := json.Marshal(esResp)
	json.Unmarshal(dataBytes, &docResp)
	json.NewEncoder(response).Encode(docResp["_id"])
}

func GetEmployeesList(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("content-type", "application/json")
	employees := make([]Employee, 0)
	ctx := context.Background()
	searchResult, err := client.Search().Index(esIndex).Query(elastic.NewMatchAllQuery()).Do(ctx)
	if err != nil {
		log.Fatal(err)
		return
	}
	if searchResult.Hits.TotalHits > 0 {
		for _, hit := range searchResult.Hits.Hits {
			var e Employee
			err := json.Unmarshal(*hit.Source, &e)
			if err != nil {
				log.Fatal(err)
			}
			employees = append(employees, e)
		}
	} else {
		err := errors.New("No records in the database")
		log.Fatal(err)
		return
	}
	json.NewEncoder(response).Encode(employees)
}
func GetEmployee(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("content-type", "application/json")
	ctx := context.Background()
	params := mux.Vars(request)
	employeeID := params["id"]
	var employee Employee
	fetchQuery := "{\"term\":{\"_id\":\"%s\"}}"
	queryString := fmt.Sprintf(fetchQuery, employeeID)
	query := elastic.RawStringQuery(queryString)
	boolQuery := elastic.NewBoolQuery().Must(query)
	esResp, err := client.Search().Index(esIndex).Type(esType).Query(boolQuery).Do(ctx)
	if err != nil {
		log.Fatal(err)
	}
	dataBytes, err := json.Marshal(esResp.Hits.Hits[0].Source)
	if err != nil {
		log.Fatalln(err)
	}
	json.Unmarshal(dataBytes, &employee)
	json.NewEncoder(response).Encode(employee)
}
func DeleteEmployee(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("content-type", "application/json")
	ctx := context.Background()
	params := mux.Vars(request)
	employeeID := params["id"]
	fetchQuery := "{\"term\":{\"_id\":\"%s\"}}"
	queryString := fmt.Sprintf(fetchQuery, employeeID)
	query := elastic.RawStringQuery(queryString)
	boolQuery := elastic.NewBoolQuery().Must(query)

	_, err := elastic.NewDeleteByQueryService(client).Index(esIndex).Type(esType).Query(boolQuery).Do(ctx)
	if err != nil {
		log.Fatal(err)
		return
	}
	json.NewEncoder(response).Encode("Success")
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalln(err)
	}
	port := os.Getenv("PORT")
	elasticURL := os.Getenv("ELASTIC_URL")
	esClient, err := elastic.NewClient(elastic.SetURL(elasticURL), elastic.SetMaxRetries(3), elastic.SetSniff(false), elastic.SetHealthcheck(false))
	if err != nil {
		log.Fatalln(err)
	}
	log.Print("application has started")
	client = esClient
	router := mux.NewRouter()
	router.HandleFunc("/employee", AddEmployeeInfo).Methods("POST")
	router.HandleFunc("/employees", GetEmployeesList).Methods("GET")
	router.HandleFunc("/employee/{id}", GetEmployee).Methods("GET")
	router.HandleFunc("/employee/{id}", DeleteEmployee).Methods("DELETE")
	if err := http.ListenAndServe(":"+port, router); err != nil {
		log.Fatalln(err)
	}

}
