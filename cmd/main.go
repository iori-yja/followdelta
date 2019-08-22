package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/kurrik/oauth1a"
	"github.com/kurrik/twittergo"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

func ParseConfig(fname string) (*twittergo.Client, error) {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		return nil, fmt.Errorf("Failed to open the configuration file %s", fname)
	}
	var conf map[string]string
	if err := json.Unmarshal(data, &conf); err != nil {
		return nil, err
	}
	config := oauth1a.ClientConfig{
		ConsumerKey:    conf["consumer_key"],
		ConsumerSecret: conf["consumer_secret"],
	}
	user := oauth1a.NewAuthorizedConfig(conf["access_token"], conf["access_token_secret"])
	client := twittergo.NewClient(&config, user)
	return client, nil
}

func UserInfo(client *twittergo.Client, ids []uint64) (map[uint64]Description, error) {
	ret := make(map[uint64]Description, len(ids))
	for i, next := 0, 100; i < len(ids); i, next = next, next+100 {
		if next > len(ids) {
			next = len(ids)
		}
		// ids for check
		cids := ids[i : next-1]
		q := ""
		for _, id := range cids {
			q = fmt.Sprintf("%s%d,", q, id)
		}
		q = strings.TrimRight(q, ",")
		req, err := http.NewRequest("GET", fmt.Sprintf("/1.1/users/lookup.json?user_id=%s", q), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create a request", err.Error())
		}
		resp, err := client.SendRequest(req)
		if err != nil {
			return nil, fmt.Errorf("request failed: %s with %v", err.Error(), cids)
		}
		var user []twittergo.User
		if err := resp.Parse(&user); err != nil {
			return nil, fmt.Errorf("parse failed: %s", err.Error())
		}
		for _, u := range user {
			ret[u.Id()] = Description{
				Name:       u.Name(),
				ScreenName: u.ScreenName(),
			}
		}
	}
	return ret, nil
}

type FollowIds struct {
	Ids        []uint64 `json:"ids"`
	NextCursor int      `json:"next_cursor"`
}

func GetFollowIds(client *twittergo.Client, cur int) (FollowIds, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("/1.1/followers/ids.json?cursor=%d", cur), nil)
	if err != nil {
		return FollowIds{}, err
	}
	resp, err := client.SendRequest(req)
	if err != nil {
		return FollowIds{}, err
	}
	defer resp.Body.Close()
	dump, err := os.Create(fmt.Sprintf("data-%s.json", time.Now().Format("2006-01-02:03:04:05")))
	if err != nil {
		return FollowIds{}, err
	}
	var buf bytes.Buffer
	var ids FollowIds
	r := io.TeeReader(resp.Body, dump)
	io.Copy(&buf, r)

	if resp.StatusCode != 200 {
		return FollowIds{}, fmt.Errorf("StatusCode: %d\nData: %s\n", resp.StatusCode, string(buf.Bytes()))
	}
	if err := json.Unmarshal(buf.Bytes(), &ids); err != nil {
		return FollowIds{}, err
	}
	return ids, nil
}

func GetAllFollowers(client *twittergo.Client) ([]uint64, error) {
	cur := -1
	ret := make([]uint64, 0, 5000)
	for {
		ids, err := GetFollowIds(client, cur)
		if err != nil {
			return ret, err
		}
		ret = append(ret, ids.Ids...)
		if ids.NextCursor == 0 {
			return ids.Ids, nil
		}
		cur = ids.NextCursor
	}
}

// aux functions
func itob(id uint64) []byte {
	bin := make([]byte, 8)
	binary.LittleEndian.PutUint64(bin, id)
	return bin
}

func btoi(id []byte) uint64 {
	return binary.LittleEndian.Uint64(id)
}

type Description struct {
	LastSeen   string `json:"last_seen"`
	FirstSeen  string `json:"first_seen"`
	Name       string `json:"name"`
	ScreenName string `json:"screen_name"`
}

func ReadPreviousFollowers(db *bolt.DB, bname string) (map[uint64]Description, error) {
	ret := make(map[uint64]Description, 4000)
	err := db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(bname)); b != nil {
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var desc Description
				err := json.Unmarshal(v, &desc)
				if err != nil {
					log.Print(err.Error())
				}
				ret[btoi(k)] = desc
			}
		}
		return nil
	})

	return ret, err
}

func WriteFollowers(db *bolt.DB, bname string, data map[uint64]Description) error {
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bname))
		if err != nil {
			return err
		}
		for k, v := range data {
			dat, err := json.Marshal(v)
			if err != nil {
				log.Print(err.Error())
			}
			if err := b.Put(itob(k), dat); err != nil {
				return err
			}
		}
		return nil
	})
}

func DeleteFollowers(db *bolt.DB, bname string, keys []uint64) error {
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bname))
		if err != nil {
			return err
		}
		for _, k := range keys {
			if err := b.Delete(itob(k)); err != nil {
				return err
			}
		}
		return nil
	})
}

func Index(client *twittergo.Client, db *bolt.DB) error {
	intoSlice := func(m map[uint64]Description) []uint64 {
		r := make([]uint64, 0, len(m))
		for k, _ := range m {
			r = append(r, k)
		}
		return r
	}
	report := func(label string, descs map[uint64]Description) {
		str, _ := json.Marshal(descs)
		fmt.Printf("{\"%s\": %s}\n", label, str)
	}

	prevDescs, err := ReadPreviousFollowers(db, "active")
	if err != nil {
		return fmt.Errorf("Failed to get previous followers: %s", err.Error())
	}

	cur, err := GetAllFollowers(client)
	if err != nil {
		return fmt.Errorf("Failed to get current followers: %s", err.Error())
	}
	ts := time.Now().Format("2006-01-02:03:04:05")
	curDescs, err := UserInfo(client, cur)
	if err != nil {
		return fmt.Errorf("Failed to describe current followers: %s", err.Error())
	}

	newbies := make(map[uint64]Description)
	for k, v := range curDescs {
		v.LastSeen = ts
		if _, ok := prevDescs[k]; !ok {
			v.FirstSeen = ts
			newbies[k] = v
		}
		curDescs[k] = v
	}
	report("newbies", newbies)

	traitors := make(map[uint64]Description)
	for k, v := range prevDescs {
		if _, ok := curDescs[k]; !ok {
			traitors[k] = v
		}
	}
	report("traitors", traitors)
	fmt.Printf("{\"delta\": %d}", len(curDescs)-len(prevDescs))

	if err := WriteFollowers(db, "active", curDescs); err != nil {
		log.Printf("Write error: %s", err.Error())
	}
	if err := DeleteFollowers(db, "active", intoSlice(traitors)); err != nil {
		log.Printf("Delete error: %s", err.Error())
	}
	if err := WriteFollowers(db, "graveyard", traitors); err != nil {
		log.Printf("Write graves error: %s", err.Error())
	}

	return err
}

func main() {
	confFile := flag.String("conf", "conf.json", "configuration file")
	dbFile := flag.String("db", "db.bolt", "db file (bolt)")
	readDb := flag.Bool("read", false, "only read db")
	grave := flag.Bool("grave", false, "only show graveyard db")
	flag.Parse()
	client, err := ParseConfig(*confFile)
	if err != nil {
		log.Printf(err.Error())
		os.Exit(1)
	}
	db, err := bolt.Open(*dbFile, 0666, nil)
	if err != nil {
		log.Printf(err.Error())
		os.Exit(1)
	}
	defer db.Close()

	if *grave {
		ids, err := ReadPreviousFollowers(db, "graveyard")
		if err != nil {
			log.Printf(err.Error())
			os.Exit(1)
		}
		dat, _ := json.Marshal(ids)
		fmt.Print(string(dat))
	} else if *readDb {
		ids, err := ReadPreviousFollowers(db, "active")
		if err != nil {
			log.Printf(err.Error())
			os.Exit(1)
		}
		dat, _ := json.Marshal(ids)
		fmt.Print(string(dat))
	} else {
		if err := Index(client, db); err != nil {
			log.Fatal(err.Error())
		}
	}
}
