package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"sort"
	"log"
	"fmt"
	"net/http"
	"flag"
	"time"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"github.com/kurrik/oauth1a"
	"github.com/kurrik/twittergo"

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
		ConsumerKey: conf["consumer_key"],
		ConsumerSecret: conf["consumer_secret"],
	}
	user := oauth1a.NewAuthorizedConfig(conf["access_token"], conf["access_token_secret"])
	client := twittergo.NewClient(&config, user)
	return client, nil
}

func UserInfo(client *twittergo.Client, ids []uint64) (map[uint64]Description, error) {
	ret := make(map[uint64]Description, len(ids))
	for i, next := 0, 100; i < len(ids); i, next = next, next + 100 {
		if next > len(ids) {
			next = len(ids)
		}
		// ids for check
		cids := ids[i:next-1]
		q := ""
		for _, id := range cids {
			q = fmt.Sprintf("%s%d,", q, id)
		}
		q = strings.TrimRight(q, ",")
		req, err := http.NewRequest("GET", fmt.Sprintf("/1.1/users/lookup.json?user_id=%s", q), nil)
		if err != nil {
			log.Printf("req failed")
			return nil, err
		}
		resp, err := client.SendRequest(req)
		if err != nil {
			log.Printf("resp failed")
			return nil, err
		}
		var user []twittergo.User
		if err := resp.Parse(&user); err != nil {
			log.Printf("parse failed")
			return nil, err
		}
		for _, u := range user {
			ret[u.Id()] = Description{
				Name: u.Name(),
				ScreenName: u.ScreenName(),
			}
		}
	}
	return ret, nil
}

type FollowIds struct {
	Ids        []uint64 `json:"ids"`
	NextCursor int     `json:"next_cursor"`
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

func Diff(cur, prev []uint64) ([]uint64, []uint64) {
	// small comes first
	sort.Slice(cur, func(i, j int) bool {return cur[i] < cur[j]})
	sort.Slice(prev, func(i, j int) bool {return prev[i] < prev[j]})

	newbies, traitors := make([]uint64, 0), make([]uint64, 0)
	i, j := 0, 0
	for ; i < len(cur) && j < len(prev); {
		if cur[i] == prev[j] {
			// She remains; skip
			i, j = i+1, j+1
		} else if cur[i] > prev[j] {
			// prev[j] didn't appear in cur
			traitors = append(traitors, prev[j])
			j = j+1
		} else {
			// cur[i] didn't appear in prev, so she is newcomer
			log.Printf("%d(%d) vs %d(%d)", cur[i], i, prev[j], j)
			newbies = append(newbies, cur[i])
			i = i+1
		}
	}
	// remaining currents are all newbies... am I right?
	newbies = append(newbies, cur[i:]...)
	traitors = append(traitors, prev[j:]...)
	return newbies, traitors
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

	cur, err := GetAllFollowers(client)
	if err != nil {
		return fmt.Errorf("Failed to get current followers: %s", err.Error())
	}
	prevDescs, err := ReadPreviousFollowers(db, "active")
	if err != nil {
		return fmt.Errorf("Failed to get previous followers: %s", err.Error())
	}
	n, t := Diff(cur, intoSlice(prevDescs))

	// Update the database
	ts := time.Now().Format("2006-01-02:03:04:05")
	curDescs, err := UserInfo(client, cur)
	if err != nil {
		return fmt.Errorf("Failed to describe current followers: %s", err.Error())
	}

	for k, v := range curDescs {
		if _, ok := prevDescs[k]; !ok {
			v.FirstSeen = ts
		}
		v.LastSeen = ts
		curDescs[k] = v
	}

	if err := WriteFollowers(db, "active", curDescs); err != nil {
		log.Printf("Write error: %s", err.Error())
	}
	if err := DeleteFollowers(db, "active", t); err != nil {
		log.Printf("Delete error: %s", err.Error())
	}
	traitors := make(map[uint64]Description, len(t))
	for _, k := range t {
		traitors[k] = prevDescs[k]
	}
	if err := WriteFollowers(db, "graveyard", traitors); err != nil {
		log.Printf("Write graves error: %s", err.Error())
	}

	newbies := make(map[uint64]Description, len(n))
	for _, k := range n {
		newbies[k] = curDescs[k]
	}
	report("newbies", newbies)
	report("traitors", traitors)
	fmt.Printf("delta: %d", len(cur) - len(prevDescs))
	return err
}

func main() {
	confFile := flag.String("conf", "", "configuration file")
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
