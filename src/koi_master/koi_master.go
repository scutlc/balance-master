package main

import (
	"conf"
	"encoding/json"
	"github.com/aiwuTech/fileLogger"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

//ip struct
type Ip struct {
	ip                 string
	status             int
	ltime              int64
	overload_transcode int
	overload_record    int
	overload_picture   int
	cluster            string
	is_master          int
}

//update ip status
func (ip Ip) UpdateStatus() {
	if ip.overload_transcode > 0 && ip.overload_record > 0 && ip.overload_picture > 0 && time.Now().Unix()-ip.ltime < 30 {
		ip.status = 0
	} else {
		ip.status = -1
	}
}

//cluster struct
type Cluster struct {
	master    string
	slave     string
	transcode int
	record    int
	picture   int
}

//var Max_Queue int = 100000
//var Task_Queue chan HttpRw
//var HeartBeat_Queue chan string
//var Max_worker int = 10
//var Max_HBeat_worker int = 10

var ClusterConf = new(conf.Config)
var DomainConf = new(conf.Config)

var Clusters = make(map[string]Cluster)
var Ips = make(map[string]*Ip)
var Domain2Cluster = make(map[string][]string)

var domains []string
var clusters []string

var logFile *fileLogger.FileLogger

func InitLog() {
	logFile = fileLogger.NewDefaultLogger("/Users/carry/gospace/koi_master/log", "koi_master.log")
	logFile.SetLogLevel(fileLogger.INFO)
}

//handleheartbeat
func Handle_HeartBeat(bossinfo map[string]interface{}) {
	var ip string
	var transcode int
	var record int
	var picture int
	if v, ok := bossinfo["ip"]; ok {
		ip = v.(string)
	} else {
		panic("ip not in heartbeat json")
	}
	if v, ok := bossinfo["transcode"]; ok {
		transcode = int(v.(float64))
	} else {
		panic("transcode not in heartbeat json")
	}
	if v, ok := bossinfo["record"]; ok {
		record = int(v.(float64))
	} else {
		panic("record not in heartbeat json")
	}
	if v, ok := bossinfo["picture"]; ok {
		picture = int(v.(float64))
	} else {
		panic("picture not in heartbeat json")
	}

	logFile.I("receive heart_beat from %v, info= %v, %v, %v", ip, transcode, record, picture)

	if v, ok := Ips[ip]; ok {
		logFile.I("before update boss status ,info=%v,%v,%v,%v", Ips[ip].overload_transcode, Ips[ip].overload_record, Ips[ip].overload_picture, Ips[ip].ltime)
		Ips[ip].overload_transcode = Clusters[v.cluster].transcode - transcode
		Ips[ip].overload_record = Clusters[v.cluster].record - record
		Ips[ip].overload_picture = Clusters[v.cluster].picture - picture
		Ips[ip].ltime = time.Now().Unix()
		logFile.I("after update boss status ,info=%v,%v,%v,%v", Ips[ip].overload_transcode, Ips[ip].overload_record, Ips[ip].overload_picture, Ips[ip].ltime)
	} else {
		logFile.E("receive heartbeat from unknown ip")
	}
}

//handleschedule
func HandleSchedule(domain string) (bool, string, [][]string) {
	var ips [][]string
	//update boss ip status
	for _, v := range Ips {
		v.UpdateStatus()
	}
	//return specify cluster machine
	if v, ok := Domain2Cluster[domain]; ok {
		if len(v) > 0 {
			for _, c := range v {
				var cip []string
				if Ips[Clusters[c].master].status == 0 {
					cip = append(cip, Clusters[c].master)
				}
				if Ips[Clusters[c].slave].status == 0 {
					cip = append(cip, Clusters[c].slave)
				}
				ips = append(ips, cip)
			}
		}
		if len(ips) > 0 {
			return true, "ok", ips
		} else {
			return false, "cannot find target boss", [][]string{}
		}
	}

	//return all aviable machine
	for _, c := range clusters {
		var cip []string
		if Ips[Clusters[c].master].status == 0 {
			cip = append(cip, Clusters[c].master)
		}
		if Ips[Clusters[c].slave].status == 0 {
			cip = append(cip, Clusters[c].slave)
		}
		ips = append(ips, cip)
	}
	if len(ips) > 0 {
		return true, "ok", ips
	} else {
		return false, "cannot find target boss", [][]string{}
	}

}

//handle new tasks
func handle_server(r1 string) (bool, string, [][]string) {
	defer func() {
		if err := recover(); err != nil {
			logFile.E(err.(string))
		}
	}()

	url := GetTaskUrl(r1)
	host := GetTaskHost(url)
	i := 0
	for _, v := range domains {
		if v == host {
			break
		}
		i = i + 1
	}
	if i >= len(domains) {
		return false, "cannot find host", [][]string{}
	} else {
		return HandleSchedule(host)
	}
}

//gettaskhost
func GetTaskHost(s string) (host string) {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	h, _ := url.ParseQuery(u.RawQuery)
	host = h["ssshost"][0]
	return host
}

//gettaskurl
func GetTaskUrl(s string) (url string) {
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		logFile.E("json dump fail")
	}
	input := result["input"]
	url = input.([]interface{})[0].(map[string]interface{})["url"].(string)
	return url
}

//handleserverrequest
func HandleServer(c http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			logFile.E(err.(string))
		}
	}()

	body, _ := ioutil.ReadAll(req.Body)
	body_str := string(body)
	logFile.I("receive new request  %v", body_str)
	f, msg, result := handle_server(body_str)
	logFile.I("handle new request,result=%v,%v", msg, result)

	if f {
		data, err := json.Marshal(result)
		c.Write([]byte(data))
		if err != nil {
			panic(err)
		}
	} else {
		logFile.E("some error appear %v", msg)
	}
}

//handleheartBeatRequest
func HandleHeartBeat(c http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			logFile.E(err.(string))
		}
	}()

	body, _ := ioutil.ReadAll(req.Body)
	body_str := string(body)
	logFile.I("receive heartbeat %v,", body_str)

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(body_str), &result); err != nil {
		panic("json dump fail")
	}
	Handle_HeartBeat(result)
}

//Handleinitconf
func Handle_Init_Conf() {
	defer func() {
		if err := recover(); err != nil {
			logFile.E(err.(string))
		}
	}()
	//init and rend clusterconf and hostconf
	ClusterConf.InitConfig("../src/conf/cluster.conf")
	DomainConf.InitConfig("../src/conf/domain.conf")
	clusters = strings.Split(ClusterConf.Read("clusters", "clusters"), ";")
	domains = strings.Split(DomainConf.Read("domains", "domains"), ";")
	//load cluster conf to struct
	for _, v := range clusters {
		var s *Cluster = new(Cluster)
		s.master = ClusterConf.Read(v, "master")
		s.slave = ClusterConf.Read(v, "slave")
		s.transcode, _ = strconv.Atoi(ClusterConf.Read(v, "transcode"))
		s.record, _ = strconv.Atoi(ClusterConf.Read(v, "record"))
		s.picture, _ = strconv.Atoi(ClusterConf.Read(v, "picture"))
		Clusters[v] = *s
		Ips[s.master] = &Ip{s.master, 0, time.Now().Unix(), 0, 0, 0, v, 1}
		Ips[s.slave] = &Ip{s.slave, 0, time.Now().Unix(), 0, 0, 0, v, 0}
	}

	//load host conf to struct
	for _, v := range domains {
		Domain2Cluster[v] = strings.Split(DomainConf.Read(v, "cluster"), ";")
	}

}

//handleReloadConf
func HandleReloadConf(c http.ResponseWriter, req *http.Request) {
	Handle_Init_Conf()
}

func main() {
	InitLog()
	Handle_Init_Conf()
	//init worker threads to handle new request
	//Task_Queue = make(chan HttpRw, Max_Queue)
	//for i := 0; i < Max_worker; i++ {
	//	go handle_server(i)
	//}
	//init worker threads to handle heartbeat
	//for i := 0; i < Max_HBeat_worker; i++ {
	//	go Handle_HeartBeat(i)
	//}
	//panic("test")
	http.HandleFunc("/create", HandleServer)
	http.HandleFunc("/reload", HandleReloadConf)
	http.HandleFunc("/heartbeat", HandleHeartBeat)
	logFile.I("listen at port 6001")
	http.ListenAndServe(":6002", nil)
}
