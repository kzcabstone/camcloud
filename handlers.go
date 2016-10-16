package camcloud

import (
	"github.com/gorilla/mux"
	"encoding/json"
	"net/http"
	"log"
	//"strconv"
)

type uploaderResponse struct {
	Status    string    `json:"status"`
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
	rec.Result_channel = rchan

	db_inserter_data_channel <- rec
	u.Status = <- rchan

	return u
}

func queryRecords(w http.ResponseWriter, r *http.Request) interface{} {
	vars := mux.Vars(r)
	cam := vars["cam"]
	vc := vars["vc"]
	vt := vars["vt"]
	vp := vars["vp"]

	if vars["suid"] == "" {
		log.Printf("GetArticlesForUser: invalid request, no suid. Ignore")
		return nil
	}
	if vars["uid"] == "" {
		log.Printf("GetArticlesForUser: invalid request, no uid. Ignore")
		return nil
	}

	suid := vars["suid"]
	if !checkSUAuth(suid) {
		log.Printf("GetArticlesForUser: auth failed %s", suid)
		return nil
	}

	/*
	uid := vars["uid"]
	u := new(get_feeds_of_user_response)

	u.Fids = getFeedsForUser(uid)
	*/
	log.Printf("queryRecords: cam %s, vc %s, vt %s, vp %s", cam, vc, vt, vp)
	
	return nil
}