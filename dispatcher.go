package camcloud

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"fmt"
)

type config struct {
	HttpPort int    `json:"http_port"`
	Suid     string `json:"su_id"`
	ClusterIpAddress string `json:"cluster_ip_address"`
	DBInserterDataChannelDepth int `json:"db_inserter_data_channel_depth"`
	DBQueryChannelDepth int `json:"db_query_channel_depth"`
}

var conf config

func commonWrapper(f func(http.ResponseWriter, *http.Request) interface{}) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		u := f(w, r)
		b, err := json.Marshal(u)
		check(err)
		w.Write(b)
	}
}

func Main() {
	/* read configuration */
	file, err := os.Open("config.json")
	defer file.Close()

	if err != nil {
		/* configure not found */
		log.Printf("Unable to read config.json. Setting default parameters.")
		conf.HttpPort = 80
		conf.ClusterIpAddress = "127.0.0.1"
    	conf.Suid = "15337"
    	conf.DBInserterDataChannelDepth = 512
	} else {
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&conf)
		if err != nil {
			log.Printf("Error reading config.json: %s", err)
			return
		}
	}

	initializeDBAccessor()

	router := mux.NewRouter()
	// Each of these handler funcs would be called inside a go routine
	router.HandleFunc("/u", commonWrapper(recordUploader)).Methods("POST")
	router.HandleFunc("/q/{cam}/{vc}/{vt}/{vp}/{tss}/{tse}",
		commonWrapper(queryRecords)).Methods("GET")
	http.Handle("/", router)

	log.Println(fmt.Sprintf("Listening at port %d ...", conf.HttpPort))
	http.ListenAndServe(fmt.Sprintf(":%d", conf.HttpPort), router)
	log.Println("Done! Exiting...")

	uninitializeDBAccessor()
}

/*
db_inserter_data_channel <- record
		status := <- result_chan
		if status != "received" {
			log.Printf("%s", status)
		}
		if counter > 500 {
			break
		}
*/