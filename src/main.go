package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/karrick/godirwalk"
	_ "github.com/mattn/go-sqlite3"
)

func Index(dirname string, drop bool) {
	// Open the connection to the local sqlite db
	db, err := sql.Open("sqlite3", "./MyDB.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create the files table
	deleteStatement := "DROP TABLE IF EXISTS files"
	createStatement := "CREATE TABLE files(path TEXT, filename TEXT, ext TEXT)"
	if drop {
		_, err = db.Exec(deleteStatement)
		if err != nil {
			log.Printf("%q: %s\n", err, deleteStatement)
			return
		}
	}

	_, err = db.Exec(createStatement)
	if err != nil {
		log.Printf("%q: %s\n", err, createStatement)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err, "COULD NOT CREATE TRANSACTION!")
	}

	// Begin a transaction
	stmt, err := tx.Prepare("INSERT INTO files(path, filename, ext) VALUES(?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	index := 1

	err = godirwalk.Walk(dirname, &godirwalk.Options{
		Callback: func(osPathname string, de *godirwalk.Dirent) error {
			osPathname = strings.ReplaceAll(osPathname, "\\", "/")
			//osPathname = strings.ReplaceAll(osPathname, "$", "_DOLLAR_")
			if de.IsDir() {
				// fmt.Printf("DIR %s %s\n", de.ModeType(), osPathname)
			} else if de.IsRegular() {
				// After 10000 files commmit the files to the db
				if index%10000 == 0 {
					// Commit the querys in the stmt variable
					fmt.Printf("Commiting: Total=%d \n", index)
					commitError := tx.Commit()
					if commitError != nil {
						log.Fatal("Commit error: ", commitError)
					}
					stmt.Close()

					tx, err = db.Begin()
					if err != nil {
						log.Fatal(err, "COULD NOT CREATE TRANSACTION!")
					}

					stmt, err = tx.Prepare("INSERT INTO files(path, filename, ext) VALUES(?, ?, ?)")
					if err != nil {
						log.Fatal(err)
					}
				}

				index++

				tmp := strings.Split(osPathname, "/")

				filename := tmp[len(tmp)-1]

				i := strings.Index(filename, ".")

				ext := ""
				if i > -1 {
					ext = filename[i:]
				}

				// Put the pathname, filename and ext into the Transaction Statement
				_, txError := stmt.Exec(osPathname, filename, ext)
				if txError != nil {
					log.Fatal("Transaction error", txError)
				}

			}

			return nil
		},
		ErrorCallback: func(osPathname string, err error) godirwalk.ErrorAction {
			// If dir or files cannot be read/opened ignore them
			fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
			return godirwalk.SkipNode
		},
		Unsorted: true, // (optional) set true for faster yet non-deterministic enumeration (see godoc)
	})

	tx.Commit()
	stmt.Close()

	if err != nil {
		panic(err)
	}
}

func Search(file string) {
	db, err := sql.Open("sqlite3", "./MyDB.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT * from files WHERE filename='%s'", file))
	if err != nil {
		log.Fatal("Query Failed: ", err)
	}
	defer rows.Close()

	for rows.Next() {
		var path string
		var filename string
		var ext string
		err = rows.Scan(&path, &filename, &ext)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s, %s, %s \n", path, filename, ext)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

}

func main() {
	Index("C:\\", true)
	// Search("main.py")
}
