package camcloud

import (
	"github.com/gorilla/mux"
	"encoding/json"
	"net/http"
	"log"
	"strconv"
)

type uploaderResponse struct {
	Status    string    `json:"status"`
}

type queryResponse struct {
	Count_of_records int `json:"count_of_records"`
	Records []CamRecord `json:"records"`
}

func recordUploader(w http.ResponseWriter, r *http.Request) interface{} {
	/* Parse JSON request */
	var rec CamRecord
	jsondecoder := json.NewDecoder(r.Body)
	u := new(uploaderResponse)
	if err := jsondecoder.Decode(&rec); err != nil {
		u.Status = "Error"
		dumpHttpRequest(r);
		return u
	}


	rchan := make(chan string)

	insert_cmd := InsertCmd {
		Record: rec,
		Result_channel: rchan,
	}

	db_inserter_data_channel <- insert_cmd
	u.Status = <- rchan

	return u
}

func queryRecords(w http.ResponseWriter, r *http.Request) interface{} {
	vars := mux.Vars(r)
	cam := vars["cam"]
	vc := vars["vc"]
	vt := vars["vt"]
	vp := vars["vp"]
	tss, err := strconv.ParseInt(vars["tss"], 10, 64) // timestamp start
	check(err)
	tse, err := strconv.ParseInt(vars["tse"], 10, 64) // timestamp end
	check(err)

	rchan := make(chan CamRecord, conf.DBInserterDataChannelDepth)

	cmd := QueryCmd {
		Cam: cam,
		Vehicle_color: vc,
		Vehicle_type: vt,
		Vehicle_plate: vp,
		Timestamp_start: tss,
		Timestamp_end: tse, 
		Result_chan: rchan,
	}
	
	db_query_channel <- cmd

	count := 0
	u := new(queryResponse)
	// read results
	for {
		r, more := <-rchan
		if more {
			count++
			u.Records = append(u.Records, r)
			//log.Printf("%#v\n", r)
		} else {
			log.Printf("Sending back %d records\n", count)
			break
		}
	}

	u.Count_of_records = count
	
	return u
}