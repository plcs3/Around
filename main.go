package main

import (
	elastic "gopkg.in/olivere/elastic.v3"
	"fmt"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"reflect"
	"github.com/pborman/uuid"
	"strings"
	"context"
	"cloud.google.com/go/storage"
	"io"
)

type Location struct{
	Lat float64  `json:"lat"`
	Lon float64  `json:"lon"`
}

type Post struct{
	User string `json:"user"`
	Message string `json:"message"`
	Location Location `json:"location"`
	Url string `json:"url"`
}

const(
	INDEX ="around"
	TYPE  ="post"
	DISTANCE="200km"
	ES_URL ="http://35.225.98.56:9200/"
	BUCKET_NAME="post-image-279107"
)

func main(){
// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{
			"mappings":{
				"post":{
					"properties":{
						"location":{
							"type":"geo_point"
						}
					}
				}
			}
		}`
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}

	fmt.Println("started-service")
	http.HandleFunc("/post",handlerPost)
	http.HandleFunc("/search",handlerSearch)
	log.Fatal(http.ListenAndServe(":8080",nil))
}

func handlerPost(w http.ResponseWriter, r *http.Request){
	w.Header().Set("Content-Type","application/json")
	w.Header().Set("Access-Control-Allow-Origin","*")
	w.Header().Set("Access-Control-Allow-Headers","Content-Type,Authorization")
	r.ParseMutipartForm(32<<20)

	fmt.Printf("Received one post request %s\n",r.FormValue("message"))
	lat,_:=strconv.ParseFloat(r.FormValue("lat"),64)
	lon,_:=strconv.ParseFloat(r.FormValue("lon"),64)
	p:=&Post{
		User:"1111",
		Message:r.FormValue("message"),
		Location:Location{
			Lat:lat,
			Lon:lon,
		},
	}

	id:=uuid.New()

	file,_,err:=r.FormFile("image")
	if err!=nil{
		http.Error(w,"Image is not available",http.StatusInternalServerError)
		fmt.Printf("Image is not avilable %v.\n",err)
		return
	}
	defer file.Close()

	ctx:=context.Background()

	_,attrs,err=:=saveToGCS(ctx,file,BUCKET_NAME,id)
	if err!=nil{
		http.Error(w,"GCS is not setup",http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v\n",err)
		return
	}

	p.Url=attrs.MediaLink

	saveToES(p,id)
}
func saveToGCS(ctx context.Context,r io.Reader,bucketName,name string)(*storage.ObjectHandle,*storage.ObjectAttrs,error){
	client,err:=storage.NewClient(ctx)
	if err!=nil{
		return nil,nil,err
	}
	defer client.Close()

	bucket:=client.Bucket(bucketName)

	if _,err=bucket.Attrs(ctx);err!=nil{
		return nil,nil,err
	}

	obj:=bucket.Object(name)
	w:=obj.NewWriter(ctx)
	if_,err:=io.Copy(w,r);err!=nil{
		return nil,nil,err
	}
	if err:=w.Close();err!=nil{
		return nil,nil,err
	}

	if err:=obj.ACL().Set(ctx,storage.AllUsers,storage.RoleReader);err!=nil{
		return nil,nil,err
	}
	attrs,err:=obj.Attrs(ctx)
	fmt.Printf("Post is saved to GCS: %S\n",attrs.MediaLink)
	return obj,attrs,err
}

func saveToES(p *Post,id string){
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL),
		elastic.SetSniff(false))
	if err != nil{
		panic(err)
	}
	_,err = es_client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()
	if err!=nil{
		panic(err)
	}
}

func handlerSearch(w http.ResponseWriter, r *http.Request){
	fmt.Println("Received one request for search.")

	lat,_ := strconv.ParseFloat(r.URL.Query().Get("lat"),64)
	lon,_ := strconv.ParseFloat(r.URL.Query().Get("lon"),64)

	ran :=DISTANCE
	if val :=r.URL.Query().Get("range");val!=""{
		ran=val+"km"
	}

	fmt.Printf("Search received: %f %f %s\n",lat,lon,ran)

	client,err:=elastic.NewClient(elastic.SetURL(ES_URL),elastic.SetSniff(false))
	if err!=nil{
		panic(err)
		return
	}
	q:=elastic.NewGeoDistanceQuery("location")
	q=q.Distance(ran).Lat(lat).Lon(lon)
	searchResult,err:=client.Search().
		Index(INDEX).
		Query(q).
		Pretty(true).
		Do()
	if err!=nil{
		panic(err)
	}
	fmt.Printf("Query took %d milliseconds\n",searchResult.TookInMillis)
	fmt.Printf("Found a total of %d post\n",searchResult.TotalHits())

	var typ Post
	var ps []Post
	for _,item := range searchResult.Each(reflect.TypeOf(typ)){
		p:=item.(Post)
		fmt.Printf("Post by %s:%s at lat %v and lon %lon\n",p.User,p.Message,p.Location.Lat,p.Location.Lon)
		if !strings.Contains(p.Message, "nigger") {
			ps=append(ps,p)
		}
	}
	js,err:=json.Marshal(ps)
	if err!=nil{
		panic(err)
		return
	}
	w.Header().Set("Content-Type","application/json")
	w.Header().Set("Access-Control-Allow-Origin","*")
	w.Write(js)
}
