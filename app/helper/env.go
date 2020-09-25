package helper

import "strings"

func SplitStringSlice(raw []string) (res []string) {
	for _, v := range raw {
		res = append(res, strings.Split(v, ",")...)
	}
	return
}
