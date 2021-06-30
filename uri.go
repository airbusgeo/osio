package osio

import "strings"

func osuriparse(scheme, uri string) (bucket, object string) {
	uri = strings.TrimPrefix(uri, scheme+"://")
	uri = strings.TrimLeft(uri, "/")
	firstSlash := strings.Index(uri, "/")
	if firstSlash == -1 {
		bucket = uri
		object = ""
	} else {
		bucket = uri[0:firstSlash]
		object = uri[firstSlash+1:]
	}
	return
}
