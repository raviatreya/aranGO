package arango

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"time"
)

type Cursor struct {
	db *Database `json:"-"`
	Id string    `json:"id"`

	Index  int           `json:"-"`
	Result []interface{} `json:"result"`
	More   bool          `json:"hasMore"`
	Amount int           `json:"count"`
	Data   Extra         `json:"extra"`
	Cached bool          `json:"cached"`

	Err    bool   `json:"error"`
	ErrMsg string `json:"errorMessage"`
	Code   int    `json:"code"`
	max    int
	Time   time.Duration `json:"time"`
}

func NewCursor(db *Database) *Cursor {
	var c Cursor
	if db == nil {
		return nil
	}
	c.db = db
	return &c
}

// Delete cursor in server and free RAM
func (c *Cursor) Delete() (bool, error) {
	if c.Id == "" {
		return false, nil
	}
	res, err := c.db.send("cursor", c.Id, "DELETE", nil, c, c)
	if err != nil {
		return false, err
	}

	switch res.Status() {
	case 202:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, nil
	}

}

func (c *Cursor) FetchBatch(r interface{}) error {
	kind := reflect.ValueOf(r).Elem().Kind()
	if kind != reflect.Slice && kind != reflect.Array {
		return errors.New("Container must be Slice of array kind")
	}
	b, err := json.Marshal(c.Result)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, r)
	if err != nil {
		return err
	}

	// fetch next batch
	if c.HasMore() {
		res, err := c.db.send("cursor", c.Id, "PUT", nil, c, c)
		if res.Status() == 200 {
			return nil
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// FetchOne iterates over cursor, returns false when no more values into batch, fetch next batch if necesary.
func (c *Cursor) FetchOne(r interface{}) bool {
	if c.Index > c.max {
		if c.More {
			//fetch rest from server
			res, err := c.db.send("cursor", c.Id, "PUT", nil, c, c)

			if err != nil {
				return false
			}

			if res.Status() == 200 {
				c.Index = 0
				return true
			} else {
				return false
			}
		} else {
			// last doc
			return false
		}
	} else {
		b, err := json.Marshal(c.Result[c.Index])
		err = json.Unmarshal(b, r)
		c.Index++ // move to next value into result
		if err != nil {
			return false
		} else {
			return true
		}
	}
}

// FetchNext is similar to FetchOne.  It is a custom implementation to access an API that exposes a bool if there are more items and an error if there was a parsing issue.
func (c *Cursor) FetchNext(r interface{}) (bool, error) {
	if c.Index >= len(c.Result) {
		if c.More {
			//fetch rest from server
			res, err := c.db.send("cursor", c.Id, "PUT", nil, c, c)

			if err != nil {
				return false, err
			}

			if res.Status() == 200 {
				c.Index = 0
			} else {
				return false, errors.New("Cursor batch request returned status code of " + strconv.Itoa(res.Status()))
			}
		} else {
			// last doc
			return false, nil
		}
	}

	b, err := json.Marshal(c.Result[c.Index])
	if err != nil {
		return false, err
	}
	err = json.Unmarshal(b, r)
	if err != nil {
		return false, err
	}
	c.Index++ // move to next value into result
	return true, nil

}

// move cursor index by 1
func (c *Cursor) Next(r interface{}) bool {
	if c.Index == c.max {
		return false
	} else {
		c.Index++
		if c.Index == c.max {
			return true
		} else {
			return false
		}
	}
}

type Extra struct {
	Stats    Stats         `json:"stats"`
	Warnings []interface{} `json:"warnings"`
}

type Stats struct {
	WritesExecuted int     `json:"writesExecuted"`
	WritesIgnored  int     `json:"writesIgnored"`
	ScannedFull    int     `json:"scannedFull"`
	ScannedIndex   int     `json:"scannedIndex"`
	Filtered       int     `json:"filtered"`
	ExecutionTime  float64 `json:"executionTime"`
	FullCount      int     `json:"fullCount"`
}

func (c Cursor) Count() int {
	return c.Amount
}

func (c *Cursor) FullCount() int {
	return c.Data.Stats.FullCount
}

func (c Cursor) HasMore() bool {
	return c.More
}

func (c Cursor) Error() bool {
	return c.Err
}

func (c Cursor) ErrCode() int {
	return c.Code
}
