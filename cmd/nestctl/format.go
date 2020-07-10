package main

import (
	"fmt"
	"text/template"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/manifoldco/promptui"
)

var FuncMap = template.FuncMap{
	"humanBytes": func(n uint64) string {
		return humanize.Bytes(n)
	},
	"bytesToString": func(b []byte) string { return string(b) },
	"shorten":       func(s string) string { return s[0:8] },
	"parseDate": func(i int64) string {
		return time.Unix(0, i).Format(time.Stamp)
	},
	"timeToDuration": func(i int64) string {
		return humanize.Time(time.Unix(i, 0))
	},
}

func ParseTemplate(body string) *template.Template {
	tpl, err := template.New("").Funcs(promptui.FuncMap).Funcs(FuncMap).Parse(fmt.Sprintf("%s\n", body))
	if err != nil {
		panic(err)
	}
	return tpl
}
