package sample

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine"
	"google.golang.org/appengine/taskqueue"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

type SqlConfig struct {
	ProjectID string
	DatasetID string
	TableID   string
	Query     string
}

func init() {
	http.HandleFunc("/autosql/task", taskEnqueueHandler)
	http.HandleFunc("/autosql/maketable", makeTableHandler)
}

func taskEnqueueHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	sc, err := getSqlConfig(ctx)
	if err != nil {
		log.Fatalf("Cannot get sql config: %v", err)
	}

	//taskqueueにpush
	task := taskqueue.NewPOSTTask("/autosql/maketable", url.Values{
		"project_id": {sc.ProjectID},
		"dataset_id": {sc.DatasetID},
		"table_id":   {sc.TableID},
		"query":      {sc.Query},
	})
	taskqueue.Add(ctx, task, "default")
}

func getSqlConfig(ctx context.Context) (SqlConfig, error) {

	bucketname := ""
	if name, ok := os.LookupEnv("BUCKET_NAME"); ok {
		bucketname = name
	}

	sqlpath := "sample/sample.sql"
	confpath := "sample/sample.yaml"

	confbuf, err := getFileGCS(ctx, bucketname, confpath)
	if err != nil {
		log.Fatalf("Cannot read object: %v", err)
	}
	var sc SqlConfig
	if err := yaml.Unmarshal(confbuf, &sc); err != nil {
		log.Fatalf("file is not in yaml format: %v", err)
	}
	log.Printf("yaml format: %v", sc)

	querybuf, err := getFileGCS(ctx, bucketname, sqlpath)
	if err != nil {
		log.Fatalf("Cannot read object: %v", err)
	}
	sc.Query = string(querybuf)

	return sc, err
}

func makeTableHandler(w http.ResponseWriter, r *http.Request) {

	ctx := appengine.NewContext(r)
	project_id := r.FormValue("project_id")
	dataset_id := r.FormValue("dataset_id")
	table_id := r.FormValue("table_id")
	query := r.FormValue("query")
	// Creates a client
	client, err := bigquery.NewClient(ctx, project_id)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	ds := client.Dataset(dataset_id)

	// query settings & run
	q := client.Query(query)
	q.Dst = ds.Table(table_id)
	q.CreateDisposition = "CREATE_IF_NEEDED"
	q.WriteDisposition = "WRITE_TRUNCATE"

	job, err := q.Run(ctx)
	if err != nil {
		// TODO handle error
	}

	// Wait until async querying is done.
	status, err := job.Wait(ctx)
	if err != nil {
		// TODO handle error
	}
	if err := status.Err(); err != nil {
		// TODO handle error
	}

	it, err := job.Read(ctx)
}

func getFileGCS(ctx context.Context, bucketname string, filepath string) ([]byte, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	rc, err := client.Bucket(bucketname).Object(filepath).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		log.Fatal(err)
	}
	return data, err
}
