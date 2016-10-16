package camcloud

import (
	"log"
	"time"
	"github.com/gocql/gocql"
	"fmt"
)

/*
type user struct {
	Id 		string 		`json:"id"`
	Feeds	map[string]bool	`json:"feeds"`
}

type feed struct {
	Id		string				`json:"id"`
	//users	map[string]bool		`json:"users"`
	Articles map[string]int64	`json:"articles"`
}
*/

type CamRecord struct {
	Cam string						`json:"c"`
	Software string					`json:"s"`
	Vehicle_color string			`json:"vc"`
	Vehicle_type string				`json:"vt"`
	Vehicle_type_score float64		`json:"vts"`
	Vehicle_plate string			`json:"vp"`
	Object_label string				`json:"ol"`
	Object_score float64			`json:"os"`
	Object_id int64					`json:"oi"`
	Object_x int 					`json:"x"`
	Object_y int 					`json:"y"`
	Object_w int 					`json:"w"`
	Object_h int 					`json:"h"`
	Result_channel chan<- string  	`json:"-"`
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
	Timestamp_start int64
	Timestamp_end int64
	Result_chan chan<- CamRecord
}

var db_session *gocql.Session
var db_inserter_data_channel chan CamRecord
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
	db_inserter_data_channel = make(chan CamRecord, conf.DBInserterDataChannelDepth) 
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

const BUFFER_SIZE = 100
var cmd_buf [BUFFER_SIZE]CamRecord
var cmd_counter = 0
func processInsertCommand(cmd CamRecord) {
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
		cmd := cmd_buf[i]
		batch.Query(
			stmt, 
			gocql.TimeUUID(),
			time.Now(),
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

	fmt.Printf("###\n")
	err := db_session.ExecuteBatch(batch)

	if err != nil {
		log.Fatal(err)
	}
}

