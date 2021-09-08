package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/awused/go-strpick/persistent"
	"github.com/mattn/go-runewidth"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.NewApp()
	app.Usage = "Selects random strings from stdin"
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "db",
			Usage: "Store persistent data in `DIR`",
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:   "clean",
			Usage:  "Read values from stdin and remove values that aren't present from the DB",
			Action: clean,
		},
		{
			Name:   "dump",
			Usage:  "Dump all values in the DB to stdin, useful for debugging",
			Action: dump,
		},
	}

	app.ArgsUsage = "[NUM]"
	app.Action = run

	err := app.Run(os.Args)
	if err != nil {
		log.Panic(err)
	}
}

func clean(c *cli.Context) error {
	p := newPicker(c)
	defer p.Close()

	readStdin(p)

	return p.CleanDB()
}

func dump(c *cli.Context) error {
	p := newPicker(c)
	defer p.Close()

	out, err := p.DumpDB()
	if err != nil {
		return err
	}

	maxwidth := 1

	for _, kv := range out {
		width := runewidth.StringWidth(kv.Key)
		if width > maxwidth {
			maxwidth = width
		}
	}

	for _, kv := range out {
		fmt.Printf("%s | %d\n", runewidth.FillRight(kv.Key, maxwidth), kv.Value)
	}

	return p.LoadDB()
}

func run(c *cli.Context) error {
	if c.NArg() < 1 {
		log.Fatal("Specify number of strings to pick")
	}

	n, err := strconv.Atoi(c.Args().First())
	if err != nil {
		return err
	}

	p := newPicker(c)
	defer p.Close()

	readStdin(p)

	next, err := p.TryUniqueN(n)
	if err != nil {
		return err
	}

	for _, v := range next {
		fmt.Println(v)
	}

	return nil
}

func newPicker(c *cli.Context) persistent.Picker {
	if c.String("db") == "" {
		log.Fatal("DB is required")
	}

	p, err := persistent.NewPicker(c.String("db"))
	if err != nil {
		log.Panic(err)
	}
	return p
}

func readStdin(p persistent.Picker) {
	s := bufio.NewScanner(os.Stdin)
	keys := []string{}
	for s.Scan() {
		keys = append(keys, s.Text())
	}

	err := p.Initialize(keys)
	if err != nil {
		log.Fatal(err)
	}
}
