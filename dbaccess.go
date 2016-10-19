package camcloud

import (
	"log"
	"github.com/gocql/gocql"
	"fmt"
	"time"
	"strings"
)

type CamRecord struct {
	Ts int64						`json:"t"`
	Cam string						`json:"c"`
	Software string					`json:"s"`
	Vehicle_color string			`json:"vc"`
	Vehicle_type string				`json:"vt"`
	Vehicle_type_score float64		`json:"vts"`
	Vehicle_plate string			`json:"vp,omitempty"`
	Object_label string				`json:"ol"`
	Object_score float64			`json:"os"`
	Object_id int					`json:"oi"`
	Object_x int 					`json:"x"`
	Object_y int 					`json:"y"`
	Object_w int 					`json:"w"`
	Object_h int 					`json:"h"`
}

type InsertCmd struct {
	Record CamRecord
	Result_channel chan<- string
	// this channel(should be buffered) is passed in by user to us,
	// we use it to send back insertion result
}

type control_cmd struct {
	cmd string
	result_chan chan<- string
}

type QueryCmd struct {
	Cam string
	Vehicle_color string
	Vehicle_type string
	Vehicle_plate string
	Timestamp_start int64 // epoch
	Timestamp_end int64 // epoch
	Result_chan chan<- CamRecord
}

var db_session *gocql.Session
var db_inserter_data_channel chan InsertCmd
var db_query_channel chan QueryCmd
var db_accessor_control_channel chan control_cmd

// Initializes the maps and the channels
func initializeDBAccessor() {
	// connect to the cluster
	cluster := gocql.NewCluster(conf.ClusterIpAddress)
	cluster.Keyspace = "camcloud"
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "yakhe",	
		Password: "qingkou",
	}
	var err error
	db_session, err = cluster.CreateSession()
	if err != nil {
		db_session.Close()
		log.Printf("initialize(): failed to create db session to cluster %s, err %s", 
			conf.ClusterIpAddress, err.Error())
	}

	// the channel only blocks if it's full
	db_inserter_data_channel = make(chan InsertCmd, conf.DBInserterDataChannelDepth) 
	db_query_channel = make(chan QueryCmd, conf.DBQueryChannelDepth)
	// control channel not buffered, so it's blocking
	db_accessor_control_channel = make(chan control_cmd) 

	// Start the mutators
	go db_inserter_routine()

	log.Printf("dbaccess: started")
}

func uninitializeDBAccessor() {
	rchan := make(chan string)
	cmd := control_cmd{
		cmd: "stop", 
		result_chan: rchan,
	}
	db_accessor_control_channel <- cmd
	status := <- rchan
	if status == "stopped" {
		db_session.Close()
		log.Printf("dbaccess: stopped")
	} else {
		log.Printf("Error: %s", status)
	}
}

func db_inserter_routine() {
	for {
		select {
			case query := <-db_query_channel:
				processQueryCommand(query)
			case data := <-db_inserter_data_channel:
				processInsertCommand(data)
			case stop := <-db_accessor_control_channel:
				if stop.cmd == "stop" {
					log.Printf("Received stop signal. Flushing buffer")
					runBatchInserts() // Flush it
					log.Printf("Flushed and stopped")
					stop.result_chan <- "stopped"
					return
				}
		}
	}
}

const BUFFER_SIZE = 200
var cmd_buf [BUFFER_SIZE]InsertCmd
var cmd_counter = 0
func processInsertCommand(cmd InsertCmd) {
	cmd_buf[cmd_counter] = cmd
	cmd.Result_channel <- "received"
	if cmd_counter == BUFFER_SIZE - 1 {
		runBatchInserts()
		cmd_counter = 0
	} else {
		cmd_counter = cmd_counter + 1
	}
}

func runBatchInserts() {
	stmt := `INSERT INTO records (
			id, 
			ts, 
			cam, 
			software, 
			vehicle_color, 
			vehicle_type, 
			vehicle_type_score,
			vehicle_plate,
			object_label,
			object_score,
			object_id,
			object_x,
			object_y,
			object_w,
			object_h
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	batch := gocql.NewBatch(gocql.LoggedBatch)
	for i := 0; i < cmd_counter; i++ {
		cmd := cmd_buf[i].Record
		batch.Query(
			stmt, 
			gocql.TimeUUID(),
			cmd.Ts,
			cmd.Cam,
			cmd.Software,
			cmd.Vehicle_color,
			cmd.Vehicle_type,
			cmd.Vehicle_type_score,
			cmd.Vehicle_plate,
			cmd.Object_label,
			cmd.Object_score,
			cmd.Object_id,
			cmd.Object_x,
			cmd.Object_y,
			cmd.Object_w,
			cmd.Object_h,
		)
	}

	fmt.Printf("inserting %d records\n", BUFFER_SIZE)
	err := db_session.ExecuteBatch(batch)

	if err != nil {
		log.Fatal(err)
	}
}

func addParamToWhereStmt(w string, param string, value string) string {
	if value != "" && strings.ToLower(value) != "all" {
		if w == "" {
			w = param + "='" + value + "'"
		} else {
			w = w + " AND " + param + "='" + value + "'"
		}
	}
	return w
}

func sendBackRecord(rec map[string]interface{}, rchan chan<- CamRecord) {
	ts := rec["ts"].(time.Time)
	crec := CamRecord {
		Ts: ts.Unix(),
		Cam: rec["cam"].(string),
		Software: rec["software"].(string),
		Object_id: rec["object_id"].(int),
		Object_x: rec["object_x"].(int),
		Object_y: rec["object_y"].(int),
		Object_w: rec["object_w"].(int),
		Object_h: rec["object_h"].(int),
		Object_label: rec["object_label"].(string),
		Object_score: rec["object_score"].(float64),
		Vehicle_type: rec["vehicle_type"].(string),
		Vehicle_type_score: rec["vehicle_type_score"].(float64),
		Vehicle_color: rec["vehicle_color"].(string),
		Vehicle_plate: rec["vehicle_plate"].(string),
	}

	rchan <- crec
}

func processQueryCommand(q QueryCmd) {
	w := ""
	w = addParamToWhereStmt(w, "cam", q.Cam)
	w = addParamToWhereStmt(w, "vehicle_color", q.Vehicle_color)
	w = addParamToWhereStmt(w, "vehicle_type", q.Vehicle_type)
	w = addParamToWhereStmt(w, "vehicle_plate", q.Vehicle_plate)
	stmt := `SELECT 
				ts, 
				cam, 
				software, 
				vehicle_color, 
				vehicle_type, 
				vehicle_type_score,
				vehicle_plate,
				object_label,
				object_score,
				object_id,
				object_x,
				object_y,
				object_w,
				object_h
			FROM records
			WHERE ` + w

	m := &map[string]interface{}{}
	iter := db_session.Query(stmt).Iter()

	for iter.MapScan(*m) {
		//fmt.Printf("%T: %#v\n", m, m)
		//fmt.Printf("%#v\n", m)
		sendBackRecord(*m, q.Result_chan)
		m = &map[string]interface{}{}
	}

	iter.Close()
	close(q.Result_chan)

	return
}

