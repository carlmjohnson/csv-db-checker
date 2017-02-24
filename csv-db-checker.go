package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

type record struct{ user, password, host, port, db string }

func run() error {
	flag.Parse()
	fname := flag.Arg(0)
	if fname == "" {
		return errors.New("Must pass in filename of CSV file to read")
	}

	records, err := recordsFromFile(fname)
	if err != nil {
		return errors.Wrap(err, "Problem getting records")
	}

	for _, v := range records {
		url := fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s?connect_timeout=5",
			v.user, v.password, v.host, v.port, v.db)

		db, err := sql.Open("postgres", url)
		if err != nil {
			fmt.Printf("✗ Could not open DB %s: %v\n", url, err)
			continue
		}

		if err = db.Ping(); err != nil {
			fmt.Printf("✗ Could not ping DB %s: %v\n", url, err)
			continue
		}

		fmt.Printf("✓ %s\n", url)
	}

	return nil
}

func recordsFromFile(fname string) ([]record, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, errors.Wrap(err, "Could not open file")
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comment = '#'

	// Discard header
	_, err = reader.Read()
	if err != nil {
		return nil, errors.Wrap(err, "Could not read first line of file")
	}

	if reader.FieldsPerRecord < 7 {
		return nil, errors.Errorf("Unexpected number of columns in file: %d", reader.FieldsPerRecord)
	}

	var records []record
	for {
		r, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "Problem parsing file")
		}
		records = append(records, record{
			user:     r[6],
			password: r[7],
			host:     r[2],
			port:     r[3],
			db:       r[5],
		})
	}

	return records, nil
}
